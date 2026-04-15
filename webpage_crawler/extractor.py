from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from bs4 import BeautifulSoup
from markdownify import markdownify as to_markdown
from playwright.async_api import Page


@dataclass(frozen=True)
class PageFeatures:
    url: str
    title: str
    section_count: int
    h1_count: int
    h2_count: int
    h3_count: int
    last_modified: str | None
    etag: str | None
    visible_text: str
    markdown: str
    links: list[str]
    interaction_snapshots: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def extract_page_features(
    page: Page,
    *,
    headers: dict[str, str] | None = None,
    interaction_snapshots: list[dict[str, Any]] | None = None,
) -> PageFeatures:
    headers = {k.lower(): v for k, v in (headers or {}).items()}
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    _clean_images(soup)

    links = await collect_href_links(page)
    title = await page.title()
    body_text = await page.locator("body").inner_text(timeout=5_000)

    return PageFeatures(
        url=page.url,
        title=title.strip(),
        section_count=len(soup.find_all("section")),
        h1_count=len(soup.find_all("h1")),
        h2_count=len(soup.find_all("h2")),
        h3_count=len(soup.find_all("h3")),
        last_modified=headers.get("last-modified"),
        etag=headers.get("etag"),
        visible_text=_compact_text(body_text),
        markdown=to_markdown(str(soup.body or soup), heading_style="ATX").strip(),
        links=sorted(set(str(link) for link in links)),
        interaction_snapshots=interaction_snapshots or [],
    )


async def snapshot_visible_state(page: Page, name: str, selector: str) -> dict[str, Any]:
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    _clean_images(soup)
    body_text = await page.locator("body").inner_text(timeout=5_000)
    links = await collect_href_links(page)
    return {
        "name": name,
        "selector": selector,
        "url": page.url,
        "title": (await page.title()).strip(),
        "visible_text": _compact_text(body_text),
        "markdown": to_markdown(str(soup.body or soup), heading_style="ATX").strip(),
        "links": sorted(set(str(link) for link in links)),
    }


def _compact_text(text: str) -> str:
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


async def collect_href_links(page: Page) -> list[str]:
    return await page.eval_on_selector_all(
        "a[href], area[href]",
        """
        elements => elements
          .filter(element => !element.closest("header, footer"))
          .map(element => element.href || element.getAttribute("href"))
          .filter(Boolean)
        """,
    )


def _clean_images(soup: BeautifulSoup) -> None:
    for img in soup.find_all("img"):
        alt = (img.get("alt") or "").strip()
        if alt:
            img.replace_with(alt)
        else:
            img.decompose()
