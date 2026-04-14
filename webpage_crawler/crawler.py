from __future__ import annotations

import asyncio
from datetime import datetime
import fnmatch
import json
import sys
import time
from collections import deque
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urlparse

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .config import SiteConfig
from .extractor import PageFeatures, extract_page_features, snapshot_visible_state

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
LANGUAGE = "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7"
class DomainPolicy:
    def __init__(
        self,
        allowed_domains: list[str],
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> None:
        self.allowed_domains = [domain.lower().lstrip(".") for domain in allowed_domains]
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []

    def allows(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        host = (parsed.hostname or "").lower()
        if not any(host == domain or host.endswith(f".{domain}") for domain in self.allowed_domains):
            return False
        if self.include_patterns and not any(fnmatch.fnmatch(url, pattern) for pattern in self.include_patterns):
            return False
        return not any(fnmatch.fnmatch(url, pattern) for pattern in self.exclude_patterns)


class WebPageCrawler:
    def __init__(self, config: SiteConfig) -> None:
        """サイト設定からドメイン制限ポリシーと待機状態を初期化します。"""
        self.config = config
        self.policy = DomainPolicy(
            config.allowed_domains,
            config.include_patterns,
            config.exclude_patterns,
        )
        self._last_access_at = 0.0

    async def crawl(self) -> list[PageFeatures]:
        """開始URLから同一ドメイン内のページをキューで巡回し、特徴量を収集します。"""
        results: list[PageFeatures] = []
        visited: set[str] = set()
        queue = deque(normalize_url(url) for url in self.config.start_urls)
        self._log(
            "crawl start: "
            f"start_urls={len(queue)} max_pages={self.config.max_pages} "
            f"domains={','.join(self.config.allowed_domains)}"
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            context = await self._new_context(browser)
            page = await context.new_page()
            page.set_default_navigation_timeout(self.config.navigation_timeout_ms)
            page.set_default_timeout(self.config.navigation_timeout_ms)

            while queue and len(results) < self.config.max_pages:
                url = queue.popleft()
                if not url or url in visited or not self.policy.allows(url):
                    continue

                visited.add(url)
                self._log(
                    f"page start: {url} "
                    f"(visited={len(visited)} collected={len(results)} queue={len(queue)})"
                )
                features = await self.extract_url(page, url)
                if features is None:
                    self._log(f"page failed: {url}")
                    continue

                results.append(features)
                added_links = 0
                for link in _feature_links(features):
                    normalized = normalize_url(link)
                    if normalized not in visited and self.policy.allows(normalized):
                        queue.append(normalized)
                        added_links += 1
                self._log(
                    f"page done: {features.url} "
                    f"title={features.title!r} links_added={added_links} "
                    f"snapshots={len(features.interaction_snapshots)} queue={len(queue)}"
                )

            await context.close()
            await browser.close()

        self._log(f"crawl done: collected={len(results)} visited={len(visited)} queue_left={len(queue)}")
        return results

    async def extract_one(self, url: str) -> PageFeatures | None:
        """指定URLを1ページだけ開き、通常クロールと同じ特徴量抽出を行います。"""
        self._log(f"extract start: {url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            context = await self._new_context(browser)
            page = await context.new_page()
            page.set_default_navigation_timeout(self.config.navigation_timeout_ms)
            page.set_default_timeout(self.config.navigation_timeout_ms)
            features = await self.extract_url(page, normalize_url(url))
            await context.close()
            await browser.close()
            self._log(f"extract done: {url} success={features is not None}")
            return features

    async def extract_url(self, page: Page, url: str) -> PageFeatures | None:
        """ページへ遷移し、表示待機・クリック操作・特徴量抽出をリトライ付きで実行します。"""
        if not self.policy.allows(url):
            self._log(f"skip domain: {url}")
            return None

        response_headers: dict[str, str] = {}
        for attempt in range(self.config.retries + 1):
            try:
                self._log(f"goto: {url} attempt={attempt + 1}/{self.config.retries + 1}")
                await self._polite_wait()
                response = await page.goto(url, wait_until="domcontentloaded")
                if response:
                    response_headers = response.headers
                    self._log(f"response: {url} status={response.status}")
                await _wait_for_network_idle(page)
                await _sleep_seconds(self.config.wait_after_load_seconds)
                snapshots = await self._run_interactions(page, url)
                features = await extract_page_features(
                    page,
                    headers=response_headers,
                    interaction_snapshots=snapshots,
                )
                return self._filter_feature_links(features)
            except Exception as exc:
                self._log(f"error: {url} attempt={attempt + 1} {type(exc).__name__}: {exc}")
                if attempt >= self.config.retries:
                    return None
                await _sleep_seconds(1.0 + attempt)
        return None

    async def write_jsonl(self, output_path: str | Path) -> None:
        """クロール結果をJSONL形式で指定パスに書き出します。"""
        results = await self.crawl()
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as file:
            for item in results:
                file.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")
        self._log(f"wrote jsonl: {path} rows={len(results)}")

    async def _new_context(self, browser: Browser) -> BrowserContext:
        """User-Agentや言語など、サイト別設定を反映したPlaywrightコンテキストを作成します。"""
        kwargs: dict[str, Any] = {
            "ignore_https_errors": True,
            "java_script_enabled": True,
            "locale": "ja-JP",
            "timezone_id": "Asia/Tokyo",
            "viewport": {"width": 1366, "height": 768},
            "extra_http_headers": {
                "Accept-Language": LANGUAGE,
                "Upgrade-Insecure-Requests": "1",
            },
            "user_agent": USER_AGENT
        }
        return await browser.new_context(**kwargs)

    async def _run_interactions(self, page: Page, base_url: str) -> list[dict[str, Any]]:
        """自動クリックと設定済みクリック操作を実行し、操作後の表示状態を収集します。"""
        snapshots: list[dict[str, Any]] = []
        if self.config.click_visible_elements:
            snapshots.extend(await self._click_visible_elements(page, base_url))

        for interaction in self.config.interactions:
            locator = page.locator(interaction.selector)
            count = await locator.count()
            self._log(f"custom interaction candidates: selector={interaction.selector!r} count={count}")
            if count == 0:
                continue

            click_count = count if interaction.all_matches else 1
            for index in range(click_count):
                target = locator.nth(index)
                if not await target.is_visible():
                    self._log(f"custom interaction skip hidden: {interaction.name} index={index + 1}/{click_count}")
                    continue
                self._log(f"custom interaction click: {interaction.name} index={index + 1}/{click_count}")
                await target.click()
                await _wait_for_network_idle(page)
                await _sleep_seconds(interaction.wait_seconds or self.config.action_wait_seconds)
                snapshots.append(
                    self._filter_snapshot_links(
                        await snapshot_visible_state(page, interaction.name, interaction.selector)
                    )
                )
        if snapshots:
            await self._goto_base_for_interactions(page, base_url)
        return snapshots

    async def _click_visible_elements(self, page: Page, base_url: str) -> list[dict[str, Any]]:
        """ユーザーに見えるクリック可能要素を順番にクリックし、表示変化をスナップショット化します。"""
        snapshots: list[dict[str, Any]] = []
        candidates = await self._visible_click_candidates(page)
        total_candidates = len(candidates)
        if self.config.max_interaction_clicks_per_page > 0:
            candidates = candidates[: self.config.max_interaction_clicks_per_page]
        self._log(
            f"auto click candidates: {len(candidates)}/{total_candidates} "
            f"base={base_url}"
        )
        for index, candidate in enumerate(candidates, start=1):
            label = _candidate_name(candidate)
            self._log(f"auto click start: {index}/{len(candidates)} {label}")
            if not await self._prepare_candidate_page(page, base_url, candidate):
                self._log(f"auto click skip stale: {index}/{len(candidates)} {label}")
                continue

            locator = page.locator(self.config.interaction_candidate_selector).nth(candidate["index"])
            try:
                if not await locator.is_visible(timeout=1_000):
                    self._log(f"auto click skip hidden: {index}/{len(candidates)} {label}")
                    continue
                before_url = page.url
                await locator.click(timeout=5_000)
                await _wait_for_network_idle(page)
                await _sleep_seconds(self.config.action_wait_seconds)
                if self.policy.allows(page.url):
                    snapshot = await snapshot_visible_state(
                        page,
                        _candidate_name(candidate),
                        self.config.interaction_candidate_selector,
                    )
                    snapshots.append(self._filter_snapshot_links(snapshot))
                    self._log(
                        f"auto click captured: {index}/{len(candidates)} "
                        f"{label} url={page.url}"
                    )
                else:
                    self._log(f"auto click outside domain after click: {index}/{len(candidates)} url={page.url}")
                if normalize_url(page.url) != normalize_url(before_url):
                    await self._goto_base_for_interactions(page, base_url)
            except Exception as exc:
                self._log(f"auto click error: {index}/{len(candidates)} {label} {type(exc).__name__}: {exc}")
                await self._goto_base_for_interactions(page, base_url)
        return snapshots

    async def _visible_click_candidates(self, page: Page) -> list[dict[str, Any]]:
        """DOM上のクリック候補から、表示中かつ同一ドメイン内の要素だけを抽出します。"""
        candidates = await page.eval_on_selector_all(
            self.config.interaction_candidate_selector,
            """
            elements => elements.map((element, index) => {
              const rect = element.getBoundingClientRect();
              const style = window.getComputedStyle(element);
              const text = (element.innerText || element.value || element.getAttribute("aria-label") || "").trim();
              const href = element.href || element.getAttribute("href") || "";
              const disabled = element.disabled || element.getAttribute("aria-disabled") === "true";
              const visible = !disabled &&
                rect.width > 0 &&
                rect.height > 0 &&
                style.visibility !== "hidden" &&
                style.display !== "none" &&
                Number(style.opacity || "1") > 0;
              return {
                index,
                tag: element.tagName.toLowerCase(),
                href,
                role: element.getAttribute("role") || "",
                ariaLabel: element.getAttribute("aria-label") || "",
                text: text.slice(0, 120),
                visible,
              };
            }).filter(candidate => candidate.visible)
            """,
        )
        filtered: list[dict[str, Any]] = []
        for candidate in candidates:
            href = str(candidate.get("href") or "")
            normalized_href = normalize_url(href) if href else ""
            if normalized_href and not self.policy.allows(normalized_href):
                continue
            filtered.append(candidate)
        return filtered

    async def _prepare_candidate_page(
        self,
        page: Page,
        base_url: str,
        candidate: dict[str, Any],
    ) -> bool:
        """クリック前に元ページへ戻し、候補要素が現在のDOMにも存在するか確認します。"""
        await self._goto_base_for_interactions(page, base_url)
        count = await page.locator(self.config.interaction_candidate_selector).count()
        return int(candidate["index"]) < count

    async def _goto_base_for_interactions(self, page: Page, base_url: str) -> None:
        """次のクリック操作に備えて、元ページへ戻るか再読み込みして表示状態を初期化します。"""
        if normalize_url(page.url) == normalize_url(base_url):
            await self._polite_wait()
            self._log(f"reload base: {base_url}")
            await page.reload(wait_until="domcontentloaded")
        else:
            await self._polite_wait()
            self._log(f"return base: {base_url} from={page.url}")
            await page.goto(base_url, wait_until="domcontentloaded")
        await _wait_for_network_idle(page)
        await _sleep_seconds(self.config.wait_after_load_seconds)

    async def _polite_wait(self) -> None:
        """アクセス間隔が設定値以上になるように待機し、最後のアクセス時刻を更新します。"""
        elapsed = time.monotonic() - self._last_access_at
        wait_seconds = self.config.request_interval_seconds - elapsed
        if wait_seconds > 0:
            self._log(f"wait: {wait_seconds:.2f}s")
            await asyncio.sleep(wait_seconds)
        self._last_access_at = time.monotonic()

    def _log(self, message: str) -> None:
        """進捗ログが有効な場合、日時付きメッセージを標準エラーへ即時出力します。"""
        timestamp = datetime.now().isoformat(timespec="seconds")
        print(f"[{timestamp}] [crawler] {message}", file=sys.stderr, flush=True)

    def _filter_feature_links(self, features: PageFeatures) -> PageFeatures:
        """抽出済み特徴量のリンクを、クロール対象ドメイン内のURLだけに絞り込みます。"""
        return replace(
            features,
            links=sorted(
                {
                    normalized
                    for link in features.links
                    if (normalized := normalize_url(link)) and self.policy.allows(normalized)
                }
            ),
            interaction_snapshots=[
                self._filter_snapshot_links(snapshot)
                for snapshot in features.interaction_snapshots
            ],
        )

    def _filter_snapshot_links(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """操作後スナップショット内のリンクを、クロール対象ドメイン内のURLだけに絞り込みます。"""
        return {
            **snapshot,
            "links": sorted(
                {
                    normalized
                    for link in snapshot.get("links", [])
                    if (normalized := normalize_url(str(link))) and self.policy.allows(normalized)
                }
            ),
        }


def _candidate_name(candidate: dict[str, Any]) -> str:
    tag = str(candidate.get("tag") or "element")
    label = str(candidate.get("text") or candidate.get("ariaLabel") or candidate.get("href") or "").strip()
    if label:
        return f"click:{tag}:{label[:60]}"
    return f"click:{tag}:{candidate.get('index')}"


def normalize_url(url: str) -> str:
    url = urldefrag(url.strip())[0]
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    scheme = parsed.scheme.lower()
    host = parsed.netloc.lower()
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{scheme}://{host}{path}{query}"


def _feature_links(features: PageFeatures) -> set[str]:
    links = set(features.links)
    for snapshot in features.interaction_snapshots:
        links.update(snapshot.get("links", []))
    return links


async def _wait_for_network_idle(page: Page) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=5_000)
    except Exception:
        pass


async def _sleep_seconds(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)
