from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class InteractionConfig:
    name: str
    selector: str
    wait_seconds: float | None = None
    all_matches: bool = False


@dataclass(frozen=True)
class SiteConfig:
    start_urls: list[str]
    allowed_domains: list[str] = field(default_factory=list)
    max_pages: int = 100
    request_interval_seconds: float = 1.0
    wait_after_load_seconds: float = 0.0
    action_wait_seconds: float = 1.0
    navigation_timeout_ms: int = 30_000
    retries: int = 2
    headless: bool = True
    user_agent: str | None = None
    accept_language: str = "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7"
    log_progress: bool = True
    click_visible_elements: bool = True
    max_interaction_clicks_per_page: int = 200
    interaction_candidate_selector: str = (
        "a[href], area[href], button, summary, "
        "[role='button'], [role='link'], "
        "input[type='button'], input[type='submit'], input[type='image'], "
        "[onclick], [tabindex]:not([tabindex='-1'])"
    )
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    interactions: list[InteractionConfig] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.start_urls:
            raise ValueError("start_urls must not be empty")
        if self.request_interval_seconds < 1.0:
            object.__setattr__(self, "request_interval_seconds", 1.0)
        if not self.allowed_domains:
            domains = sorted({urlparse(url).hostname or "" for url in self.start_urls})
            object.__setattr__(self, "allowed_domains", [d for d in domains if d])

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SiteConfig":
        interactions = [
            InteractionConfig(
                name=str(item.get("name") or item["selector"]),
                selector=str(item["selector"]),
                wait_seconds=item.get("wait_seconds"),
                all_matches=bool(item.get("all_matches", False)),
            )
            for item in data.get("interactions", [])
        ]
        return cls(
            start_urls=list(data["start_urls"]),
            allowed_domains=list(data.get("allowed_domains", [])),
            max_pages=int(data.get("max_pages", 100)),
            request_interval_seconds=float(data.get("request_interval_seconds", 1.0)),
            wait_after_load_seconds=float(data.get("wait_after_load_seconds", 0.0)),
            action_wait_seconds=float(data.get("action_wait_seconds", 1.0)),
            navigation_timeout_ms=int(data.get("navigation_timeout_ms", 30_000)),
            retries=int(data.get("retries", 2)),
            headless=bool(data.get("headless", True)),
            user_agent=data.get("user_agent"),
            accept_language=str(data.get("accept_language", "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7")),
            log_progress=bool(data.get("log_progress", True)),
            click_visible_elements=bool(data.get("click_visible_elements", True)),
            max_interaction_clicks_per_page=int(data.get("max_interaction_clicks_per_page", 200)),
            interaction_candidate_selector=str(
                data.get(
                    "interaction_candidate_selector",
                    (
                        "a[href], area[href], button, summary, "
                        "[role='button'], [role='link'], "
                        "input[type='button'], input[type='submit'], input[type='image'], "
                        "[onclick], [tabindex]:not([tabindex='-1'])"
                    ),
                )
            ),
            include_patterns=list(data.get("include_patterns", [])),
            exclude_patterns=list(data.get("exclude_patterns", [])),
            interactions=interactions,
        )


def load_site_config(path: str | Path) -> SiteConfig:
    with Path(path).open("r", encoding="utf-8") as file:
        return SiteConfig.from_dict(json.load(file))
