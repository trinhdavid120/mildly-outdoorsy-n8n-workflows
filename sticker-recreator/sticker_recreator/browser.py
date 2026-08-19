"""
Playwright stealth browser.

This is the "browser tricks to get over the bot blocking" layer. Etsy (PerimeterX)
and Amazon ("Robot Check" / CAPTCHA) both fingerprint headless automation, so we:

* drive a *real* Chromium build, not the headless shell, with the
  ``AutomationControlled`` blink feature disabled;
* patch the obvious automation tells (``navigator.webdriver``, empty plugin list,
  missing ``window.chrome``, WebGL vendor) before any page script runs;
* present a plausible desktop fingerprint (UA, locale, timezone, viewport,
  ``Accept-Language``);
* move/scroll like a human and jitter every wait;
* optionally route through a residential/rotating proxy;
* detect challenge pages and back off instead of hammering.

None of this requires patching Chromium — everything is init-script + context
options, which is what keeps it maintainable when the sites change.

Requires ``playwright`` (see requirements.txt). The offline seed path does **not**
import this module, so the pipeline still runs with nothing installed.
"""

from __future__ import annotations

import contextlib
import os
import random
from dataclasses import dataclass
from typing import Iterator, List, Optional

# Playwright is an optional dependency: only needed for live scraping. Import it
# lazily so `--source seed` works on a bare Python install.
try:  # pragma: no cover - import guard
    from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright
except Exception:  # pragma: no cover
    Browser = BrowserContext = Page = None  # type: ignore
    sync_playwright = None  # type: ignore


# A small pool of realistic recent desktop Chrome UAs. Rotate per session.
USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

VIEWPORTS = [(1440, 900), (1536, 864), (1920, 1080), (1366, 768)]

# Signatures that mean "you have been challenged / blocked", not real content.
CHALLENGE_MARKERS = (
    "captcha",
    "px-captcha",
    "are you a human",
    "unusual traffic",
    "enter the characters you see below",
    "robot check",
    "to discuss automated access",
    "access to this page has been denied",
)

# Injected before any page JS runs — hides the standard automation tells.
STEALTH_INIT_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', {
  get: () => [1, 2, 3, 4, 5].map(i => ({ name: 'Plugin ' + i })),
});
window.chrome = window.chrome || { runtime: {} };
const _query = window.navigator.permissions && window.navigator.permissions.query;
if (_query) {
  window.navigator.permissions.query = (p) =>
    p && p.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : _query(p);
}
const getParameter = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function (p) {
  if (p === 37445) return 'Intel Inc.';              // UNMASKED_VENDOR_WEBGL
  if (p === 37446) return 'Intel Iris OpenGL Engine'; // UNMASKED_RENDERER_WEBGL
  return getParameter.call(this, p);
};
"""


class ChallengeDetected(RuntimeError):
    """Raised when a page looks like a bot-block / CAPTCHA wall."""


@dataclass
class BrowserConfig:
    headless: bool = True
    proxy: Optional[str] = None          # e.g. "http://user:pass@host:port"
    slow_mo_ms: int = 0
    locale: str = "en-US"
    timezone: str = "America/Los_Angeles"
    # Min/max jitter applied to every human-paced wait, in milliseconds.
    jitter_ms: tuple[int, int] = (450, 1400)

    @classmethod
    def from_env(cls) -> "BrowserConfig":
        return cls(
            headless=os.environ.get("SCRAPER_HEADLESS", "1") != "0",
            proxy=os.environ.get("SCRAPER_PROXY") or None,
            slow_mo_ms=int(os.environ.get("SCRAPER_SLOWMO_MS", "0") or "0"),
        )


def _chromium_executable() -> Optional[str]:
    """Prefer an explicit path, then the pre-provisioned Chromium in this image."""
    explicit = os.environ.get("PLAYWRIGHT_CHROMIUM_PATH")
    if explicit and os.path.exists(explicit):
        return explicit
    for candidate in ("/opt/pw-browsers/chromium", "/opt/pw-browsers/chromium/chrome"):
        if os.path.exists(candidate):
            return candidate
    return None  # let Playwright resolve its own download


class StealthBrowser:
    """Context manager that yields stealth-configured Playwright pages."""

    def __init__(self, config: Optional[BrowserConfig] = None):
        if sync_playwright is None:
            raise RuntimeError(
                "playwright is not installed. Run `pip install -r requirements.txt` "
                "and `playwright install chromium`, or use `--source seed` for the "
                "offline path."
            )
        self.config = config or BrowserConfig.from_env()
        self._pw = None
        self._browser: Optional["Browser"] = None
        self._context: Optional["BrowserContext"] = None

    # -- lifecycle -------------------------------------------------------
    def __enter__(self) -> "StealthBrowser":
        self._pw = sync_playwright().start()
        launch_kwargs = {
            "headless": self.config.headless,
            "slow_mo": self.config.slow_mo_ms,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--start-maximized",
            ],
        }
        exe = _chromium_executable()
        if exe:
            launch_kwargs["executable_path"] = exe
        if self.config.proxy:
            launch_kwargs["proxy"] = {"server": self.config.proxy}

        self._browser = self._pw.chromium.launch(**launch_kwargs)
        width, height = random.choice(VIEWPORTS)
        self._context = self._browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale=self.config.locale,
            timezone_id=self.config.timezone,
            viewport={"width": width, "height": height},
            device_scale_factor=random.choice([1, 2]),
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        self._context.add_init_script(STEALTH_INIT_JS)
        return self

    def __exit__(self, *exc) -> None:
        for closer in (self._context, self._browser, self._pw):
            with contextlib.suppress(Exception):
                if closer is self._pw:
                    closer.stop()  # type: ignore[union-attr]
                else:
                    closer.close()  # type: ignore[union-attr]

    # -- helpers ---------------------------------------------------------
    @contextlib.contextmanager
    def page(self) -> Iterator["Page"]:
        assert self._context is not None
        page = self._context.new_page()
        try:
            yield page
        finally:
            with contextlib.suppress(Exception):
                page.close()

    def jitter(self, page: "Page") -> None:
        """A human-length pause; scale multiples of this for read/scroll time."""
        low, high = self.config.jitter_ms
        page.wait_for_timeout(random.randint(low, high))

    def humanize(self, page: "Page") -> None:
        """Small mouse move + scroll so the session doesn't look scripted."""
        with contextlib.suppress(Exception):
            page.mouse.move(random.randint(80, 700), random.randint(80, 500))
            page.mouse.wheel(0, random.randint(400, 1600))
        self.jitter(page)

    def goto(self, page: "Page", url: str, wait_selector: Optional[str] = None) -> None:
        """Navigate, then assert we didn't hit a challenge wall."""
        page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        self.humanize(page)
        if wait_selector:
            with contextlib.suppress(Exception):
                page.wait_for_selector(wait_selector, timeout=15_000)
        self.assert_not_challenged(page)

    def assert_not_challenged(self, page: "Page") -> None:
        title = (page.title() or "").lower()
        # Only sample the top of the document; full body text is expensive.
        with contextlib.suppress(Exception):
            body = (page.inner_text("body")[:2000] or "").lower()
        haystack = f"{title}\n{body}"
        for marker in CHALLENGE_MARKERS:
            if marker in haystack:
                raise ChallengeDetected(
                    f"bot-block page detected (marker={marker!r}) at {page.url}"
                )
