from __future__ import annotations

import asyncio
import fnmatch
import json
import sys
import time
from collections import deque
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Page, Route, async_playwright

from .config import SiteConfig
from .crawler import DomainPolicy, normalize_url
from .extractor import PageFeatures, extract_page_features, snapshot_visible_state

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
LANGUAGE = "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7"


class WebPageCrawlerSteps:
    """Depth-aware crawler that processes first-level, second-level, and deeper URLs in order."""

    def __init__(self, config: SiteConfig) -> None:
        """サイト設定から深さ別クロール用のポリシーと待機状態を初期化"""
        self.config = config
        self.policy = DomainPolicy(
            config.allowed_domains,
            config.include_patterns,
            config.exclude_patterns,
        )
        self._last_access_at = 0.0

    async def crawl(self, checkpoint_path: str | Path | None = None) -> list[PageFeatures]:
        """URLを深さ付きキューで処理し、階層が切り替わるたびに任意で途中保存"""
        results: list[PageFeatures] = []
        visited: set[str] = set()
        queued: set[str] = set()
        queue: deque[tuple[str, int]] = deque()
        current_depth: int | None = None
        checkpoint = Path(checkpoint_path) if checkpoint_path else None

        for url in self.config.start_urls:
            normalized = normalize_url(url)
            if normalized and self.policy.allows(normalized):
                queue.append((normalized, 0))
                queued.add(normalized)

        self._log(
            "crawl steps start: "
            f"start_urls={len(queue)} max_pages={self.config.max_pages} "
            f"max_depth={self.config.max_depth} click_max_depth={self.config.click_max_depth}"
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            context = await self._new_context(browser)
            page = await context.new_page()
            page.set_default_navigation_timeout(self.config.navigation_timeout_ms)
            page.set_default_timeout(self.config.navigation_timeout_ms)

            while queue and len(results) < self.config.max_pages:
                url, depth = queue.popleft()
                if url in visited:
                    continue
                if depth > self.config.max_depth:
                    self._log(f"page skip depth: depth={depth} url={url}")
                    continue
                if current_depth is None:
                    current_depth = depth
                elif depth != current_depth:
                    if checkpoint:
                        self._write_jsonl_sync(checkpoint, results)
                        self._log(
                            f"depth checkpoint saved: depth={current_depth} "
                            f"rows={len(results)} path={checkpoint}"
                        )
                    current_depth = depth

                visited.add(url)
                self._log(
                    f"page start: depth={depth} url={url} "
                    f"(visited={len(visited)} collected={len(results)} queue={len(queue)})"
                )

                features = await self.extract_url(page, url, depth, queue, queued, visited)
                if features is None:
                    self._log(f"page failed: depth={depth} url={url}")
                    continue

                results.append(features)
                self._log(
                    f"page done: depth={depth} url={features.url} "
                    f"title={features.title!r} success_count={len(results)} "
                    f"snapshots={len(features.interaction_snapshots)} queue={len(queue)}"
                )

            await context.close()
            await browser.close()

        reason = "max_pages" if len(results) >= self.config.max_pages else "queue_empty"
        if checkpoint:
            self._write_jsonl_sync(checkpoint, results)
            self._log(
                f"final checkpoint saved: reason={reason} "
                f"depth={current_depth} rows={len(results)} path={checkpoint}"
            )
        self._log(
            f"steps crawl done: reason={reason} collected={len(results)} "
            f"visited={len(visited)} queue_left={len(queue)}"
        )
        return results


    async def extract_one(self, url: str) -> PageFeatures | None:
        """指定URLを1ページだけ、通常の抽出処理を行う。"""
        self._log(f"steps extract start: {url}")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            context = await self._new_context(browser)
            page = await context.new_page()
            page.set_default_navigation_timeout(self.config.navigation_timeout_ms)
            page.set_default_timeout(self.config.navigation_timeout_ms)
            queue: deque[tuple[str, int]] = deque()
            features = await self.extract_url(page, normalize_url(url), 0, queue, set(), set())
            await context.close()
            await browser.close()
            self._log(f"steps extract done: {url} success={features is not None}")
            return features

    async def extract_url(
        self,
        page: Page,
        url: str,
        depth: int,
        queue: deque[tuple[str, int]],
        queued: set[str],
        visited: set[str],
    ) -> PageFeatures | None:
        """ページを取得し、表示中リンクを先にキューへ入れてから必要に応じてクリックする。"""
        if not self.policy.allows(url):
            self._log(f"skip policy: depth={depth} url={url}")
            return None

        response_headers: dict[str, str] = {}
        for attempt in range(self.config.retries + 1):
            try:
                self._log(f"goto: depth={depth} url={url} attempt={attempt + 1}/{self.config.retries + 1}")
                await self._polite_wait()
                response = await page.goto(url, wait_until="domcontentloaded")
                if response:
                    response_headers = response.headers
                    self._log(f"response: depth={depth} url={url} status={response.status}")
                await _wait_for_network_idle(page)
                await _sleep_seconds(self.config.wait_after_load_seconds)

                base_features = self._filter_feature_links(
                    await extract_page_features(page, headers=response_headers)
                )
                if self.config.enqueue_page_links_before_clicks:
                    self._enqueue_links(
                        base_features.links,
                        depth + 1,
                        queue,
                        queued,
                        visited,
                        source="page",
                    )

                snapshots: list[dict[str, Any]] = []
                if self._should_click(url, depth):
                    snapshots = await self._run_interactions(page, url, depth)
                    snapshot_links = _snapshot_links(snapshots)
                    self._enqueue_links(
                        snapshot_links,
                        depth + 1,
                        queue,
                        queued,
                        visited,
                        source="snapshot",
                    )
                else:
                    self._log(f"click skipped: depth={depth} url={url}")

                return replace(base_features, interaction_snapshots=snapshots)
            except Exception as exc:
                self._log(f"error: depth={depth} url={url} attempt={attempt + 1} {type(exc).__name__}: {exc}")
                if attempt >= self.config.retries:
                    return None
                await _sleep_seconds(1.0 + attempt)
        return None

    async def write_jsonl(self, output_path: str | Path) -> None:
        """階層ごとの途中保存を有効にしてクロールし、最終結果もJSONLに上書き保存　"""
        path = Path(output_path)
        results = await self.crawl(checkpoint_path=path)
        self._write_jsonl_sync(path, results)
        self._log(f"wrote jsonl: {path} rows={len(results)}")

    async def _new_context(self, browser: Browser) -> BrowserContext:
        """User-Agentや言語など、サイト取得用のPlaywrightコンテキストを作成　"""
        context = await browser.new_context(
            ignore_https_errors=True,
            java_script_enabled=True,
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            viewport={"width": 1366, "height": 768},
            extra_http_headers={
                "Accept-Language": LANGUAGE,
                "Upgrade-Insecure-Requests": "1",
            },
            user_agent=USER_AGENT,
        )
        await self._route_blocked_resources(context)
        return context

    async def _route_blocked_resources(self, context: BrowserContext) -> None:
        """設定された画像・フォントなどの不要リソースを読み込み前にブロックします。"""
        blocked_types = set(self.config.block_resource_types)
        blocked_patterns = self.config.block_url_patterns
        if not blocked_types and not blocked_patterns:
            return

        async def handler(route: Route) -> None:
            request = route.request
            if request.resource_type in blocked_types or any(
                fnmatch.fnmatch(request.url.lower(), pattern.lower())
                for pattern in blocked_patterns
            ):
                await route.abort()
                return
            await route.continue_()

        await context.route("**/*", handler)

    async def _run_interactions(self, page: Page, base_url: str, depth: int) -> list[dict[str, Any]]:
        """自動クリックと設定済み操作を実行し、クリック後の表示状態を収集する。"""
        snapshots: list[dict[str, Any]] = []
        if self.config.click_visible_elements:
            snapshots.extend(await self._click_visible_elements(page, base_url, depth))

        for interaction in self.config.interactions:
            locator = page.locator(interaction.selector)
            count = await locator.count()
            self._log(
                f"custom interaction candidates: depth={depth} "
                f"selector={interaction.selector!r} count={count}"
            )
            if count == 0:
                continue
            click_count = count if interaction.all_matches else 1
            for index in range(click_count):
                target = locator.nth(index)
                if not await target.is_visible():
                    continue
                await target.click()
                await _wait_for_network_idle(page)
                await _sleep_seconds(interaction.wait_seconds or self.config.action_wait_seconds)
                snapshots.append(
                    self._filter_snapshot_links(
                        await snapshot_visible_state(page, interaction.name, interaction.selector)
                    )
                )

        if snapshots and normalize_url(page.url) != normalize_url(base_url):
            await self._goto_base_for_interactions(page, base_url)
        return snapshots

    async def _click_visible_elements(self, page: Page, base_url: str, depth: int) -> list[dict[str, Any]]:
        """表示中のクリック候補を順番に押し、遷移先や表示変化をスナップショットをとる。"""
        snapshots: list[dict[str, Any]] = []
        candidates = await self._visible_click_candidates(page)
        total_candidates = len(candidates)
        if self.config.max_interaction_clicks_per_page > 0:
            candidates = candidates[: self.config.max_interaction_clicks_per_page]
        self._log(
            f"auto click candidates: depth={depth} {len(candidates)}/{total_candidates} base={base_url}"
        )

        for index, candidate in enumerate(candidates, start=1):
            label = _candidate_name(candidate)
            self._log(f"auto click start: depth={depth} {index}/{len(candidates)} {label}")
            if not await self._prepare_candidate_page(page, base_url, candidate):
                self._log(f"auto click skip stale: depth={depth} {index}/{len(candidates)} {label}")
                continue

            locator = page.locator(self.config.interaction_candidate_selector).nth(candidate["index"])
            try:
                if not await locator.is_visible(timeout=1_000):
                    continue
                before_url = page.url
                await locator.click(timeout=5_000)
                await _wait_for_network_idle(page)
                await _sleep_seconds(self.config.action_wait_seconds)
                if self.policy.allows(page.url):
                    snapshot = self._filter_snapshot_links(
                        await snapshot_visible_state(
                            page,
                            _candidate_name(candidate),
                            self.config.interaction_candidate_selector,
                        )
                    )
                    snapshots.append(snapshot)
                    self._log(
                        f"auto click captured: depth={depth} {index}/{len(candidates)} "
                        f"{label} url={page.url} snapshot_url={snapshot.get('url')}"
                    )
                else:
                    self._log(f"auto click outside policy: depth={depth} url={page.url}")
                if normalize_url(page.url) != normalize_url(before_url):
                    await self._goto_base_for_interactions(page, base_url)
            except Exception as exc:
                self._log(
                    f"auto click error: depth={depth} {index}/{len(candidates)} "
                    f"{label} {type(exc).__name__}: {exc}"
                )
                await self._goto_base_for_interactions(page, base_url)
        return snapshots

    async def _visible_click_candidates(self, page: Page) -> list[dict[str, Any]]:
        """DOMから可視クリック候補を抽出し、対象外ドメインやheader/footer配下を除外する。"""
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
                !element.closest("header, footer") &&
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

    async def _prepare_candidate_page(self, page: Page, base_url: str, candidate: dict[str, Any]) -> bool:
        """クリック前に元ページへ戻し、候補要素が現在のDOMにも残っているか確認する。"""
        if normalize_url(page.url) != normalize_url(base_url):
            await self._goto_base_for_interactions(page, base_url)
        count = await page.locator(self.config.interaction_candidate_selector).count()
        return int(candidate["index"]) < count

    async def _goto_base_for_interactions(self, page: Page, base_url: str) -> None:
        """次のクリック操作のため、元ページへ戻るか再読み込みして状態を整える。"""
        await self._polite_wait()
        if normalize_url(page.url) == normalize_url(base_url):
            self._log(f"reload base: {base_url}")
            await page.reload(wait_until="domcontentloaded")
        else:
            self._log(f"return base: {base_url} from={page.url}")
            await page.goto(base_url, wait_until="domcontentloaded")
        await _wait_for_network_idle(page)
        await _sleep_seconds(self.config.wait_after_load_seconds)

    async def _polite_wait(self) -> None:
        """アクセス間隔が設定秒数以上になるように待機します。"""
        elapsed = time.monotonic() - self._last_access_at
        wait_seconds = self.config.request_interval_seconds - elapsed
        if wait_seconds > 0:
            self._log(f"wait: {wait_seconds:.2f}s")
            await asyncio.sleep(wait_seconds)
        self._last_access_at = time.monotonic()

    def _enqueue_links(
        self,
        links: set[str] | list[str],
        depth: int,
        queue: deque[tuple[str, int]],
        queued: set[str],
        visited: set[str],
        *,
        source: str,
    ) -> None:
        """発見したリンクを深さ付きでキューへ追加し、追加/除外件数をログに出力"""
        if depth > self.config.max_depth:
            self._log(f"enqueue skipped depth: source={source} depth={depth} count={len(links)}")
            return

        raw_count = 0
        added = 0
        skipped_seen = 0
        skipped_policy = 0
        for link in links:
            raw_count += 1
            normalized = normalize_url(str(link))
            if not normalized or not self.policy.allows(normalized):
                skipped_policy += 1
                continue
            if normalized in visited or normalized in queued:
                skipped_seen += 1
                continue
            queue.append((normalized, depth))
            queued.add(normalized)
            added += 1
            self._log(f"link queued: source={source} depth={depth} url={normalized}")

        self._log(
            f"enqueue done: source={source} depth={depth} raw_links={raw_count} "
            f"links_added={added} links_skip_seen={skipped_seen} "
            f"links_skip_policy={skipped_policy} queue={len(queue)}"
        )

    def _should_click(self, url: str, depth: int) -> bool:
        """現在の深さやno_click_patternsに基づき、そのページでクリック操作するか判定する。"""
        if not self.config.click_visible_elements and not self.config.interactions:
            return False
        if depth > self.config.click_max_depth:
            return False
        return not any(fnmatch.fnmatch(url, pattern) for pattern in self.config.no_click_patterns)

    def _filter_feature_links(self, features: PageFeatures) -> PageFeatures:
        """抽出済みページ特徴量のリンクを、クロール対象URLだけに絞り込む。"""
        return replace(
            features,
            links=sorted(
                {
                    normalized
                    for link in features.links
                    if (normalized := normalize_url(str(link))) and self.policy.allows(normalized)
                }
            ),
            interaction_snapshots=[
                self._filter_snapshot_links(snapshot)
                for snapshot in features.interaction_snapshots
            ],
        )


    def _filter_snapshot_links(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """クリック後スナップショット内のURLとリンクを、クロール対象URLだけに絞り込む。"""
        snapshot_url = normalize_url(str(snapshot.get("url") or ""))
        return {
            **snapshot,
            "url": snapshot_url if snapshot_url and self.policy.allows(snapshot_url) else None,
            "links": sorted(
                {
                    normalized
                    for link in snapshot.get("links", [])
                    if (normalized := normalize_url(str(link))) and self.policy.allows(normalized)
                }
            ),
        }

    def _log(self, message: str) -> None:
        """日時付きの進捗ログを標準エラーへ即時出力"""
        timestamp = datetime.now().isoformat(timespec="seconds")
        print(f"[{timestamp}] [steps-crawler] {message}", file=sys.stderr, flush=True)

    def _write_jsonl_sync(self, output_path: Path, results: list[PageFeatures]) -> None:
        """現在までの取得結果をJSONLとして上書き保存"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as file:
            for item in results:
                file.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")


def _snapshot_links(snapshots: list[dict[str, Any]]) -> set[str]:
    links: set[str] = set()
    for snapshot in snapshots:
        if snapshot.get("url"):
            links.add(str(snapshot["url"]))
        links.update(str(link) for link in snapshot.get("links", []))
    return links


def _candidate_name(candidate: dict[str, Any]) -> str:
    tag = str(candidate.get("tag") or "element")
    label = str(candidate.get("text") or candidate.get("ariaLabel") or candidate.get("href") or "").strip()
    if label:
        return f"click:{tag}:{label[:60]}"
    return f"click:{tag}:{candidate.get('index')}"


async def _wait_for_network_idle(page: Page) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=5_000)
    except Exception:
        pass


async def _sleep_seconds(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)
