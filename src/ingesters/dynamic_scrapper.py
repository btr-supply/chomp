import random
from asyncio import sleep, gather, Task
from typing import Any, Literal, Optional

from ..models.base import ResourceField
from ..models.ingesters import Ingester
from ..utils import log_error
from ..actions import scheduler

# Import playwright types directly - mypy will be happy
from playwright.async_api import async_playwright, ViewportSize, Page, Locator, Browser, Playwright, ElementHandle

SelectorType = Literal["auto", "css", "xpath", "id", "name", "class", "role",
                       "text", "title", "href", "partial_link_text",
                       "link_text"]

__all__ = ["schedule"]


class Puppet:
  """Helper class for managing browser automation for individual fields."""
  by_id: dict[str, "Puppet"] = {}

  def __init__(self, field: ResourceField, ingester: Ingester,
               play: Playwright):
    Puppet.by_id[field.target_id] = self
    self.field = field
    self.ingester = ingester
    self.play = play
    self.browser: Optional[Browser] = None
    self.pages: list[Page] = []
    self.selector: Optional[Locator] = None
    self.elements: list[ElementHandle] = []

  @staticmethod
  async def from_field(field: ResourceField, ingester: Ingester,
                       play: Playwright) -> "Puppet":
    p = Puppet(field, ingester, play)
    for a in field.actions:
      await p.act(a)
    return p

  async def kill(self):
    if self.browser:
      await self.browser.close()
    await gather(*[p.close() for p in self.pages])

  async def ensure_browser(self, browser="chromium") -> Browser:
    if not self.browser:
      self.browser = await self.play[browser].launch()
    return self.browser

  async def ensure_page(self, target=None) -> Page:
    target = target or self.field.target or "about:blank"
    if not self.pages:
      page = await (await self.ensure_browser()).new_page()
      await page.goto(target)
      self.pages = [page]
    return self.pages[-1]

  async def ensure_selected(self, selector="html", by="auto") -> Locator:
    if not self.selector:
      await self.ensure_page()
      self.selector = await self.select(selector=selector, by=by)
    assert self.selector is not None
    return self.selector

  async def ensure_elements(self) -> list[ElementHandle]:
    if not self.elements:
      self.elements = await (await self.ensure_selected()).element_handles()
    return self.elements

  async def ensure_contents(self) -> list[Any]:
    if not self.elements:
      await self.ensure_elements()
    return await gather(*[e.text_content() for e in self.elements])

  # TODO: add support for regex
  async def select(self,
                   selector: str = "html",
                   by: SelectorType | Any = "auto") -> Locator:
    page = await self.ensure_page()
    locator: Locator
    match by:
      case "auto":
        locator = page.locator(selector)
      case "css":
        locator = page.locator(f"css={selector}")
      case "xpath":
        locator = page.locator(f"xpath={selector}")
      case "id":
        locator = page.locator(f"id={selector}")
      case "name":
        locator = page.locator(f"name={selector}")
      case "class":
        locator = page.locator(f"class={selector}")
      case "text" | "value":
        locator = page.get_by_text(selector)
      case "strict_text" | "strict_value":
        locator = page.locator(f"text={selector}")
      case "role":
        # Cast to AriaRole literal type for type safety
        locator = page.get_by_role(selector)
      case "strict_role":
        locator = page.locator(f"[role={selector}]")
      case "alt_text":
        locator = page.get_by_alt_text(selector)
      case "strict_alt_text":
        locator = page.locator(f"[alt={selector}]")
      case "title":
        locator = page.get_by_title(selector)
      case "strict_title":
        locator = page.locator(f"title={selector}")
      case "label":
        locator = page.get_by_label(selector)
      case "strict_label":
        locator = page.locator(f"label={selector}")
      case "placeholder":
        locator = page.get_by_placeholder(selector)
      case "strict_placeholder":
        locator = page.locator(f"placeholder={selector}")
      case _:
        locator = page.locator(f"[{by}={selector}]" if "data-" in
                               by else f"[data-{by}={selector}]")
    self.selector = locator
    return locator

  async def act(self, action: str, *args):
    """Executes a state-changing action on the browser and returns the modified browser."""

    part, command = action.split(":")
    try:
      match part:
        case "browser":
          match command:
            case "use":  # supports chromium, firefox, webkit, msedge (chromium based)
              self.browser = await self.play[
                  args[0] if args else "chromium"].launch(
                      *args[1:])  # --mute-audio --no-sandbox by default
            case "close":
              if self.browser:
                await self.browser.close()
            case _:
              log_error(f"Unknown browser action: {action}")

        case "page":
          match command:
            case "new":
              self.pages.append(await (await self.ensure_browser()).new_page())
            case "close":
              if self.pages:
                await self.pages.pop().close()
            case "set_viewport_size":
              page = await self.ensure_page()
              await page.set_viewport_size(
                  ViewportSize(width=int(args[0]), height=int(args[1])))
            case "goto":
              page = await self.ensure_page()
              await page.goto(args[0])
            case "go_back":
              page = await self.ensure_page()
              await page.go_back()
            case "go_forward":
              page = await self.ensure_page()
              await page.go_forward()
            case "reload":
              page = await self.ensure_page()
              await page.reload()
            case "add_init_script":
              page = await self.ensure_page()
              await page.add_init_script(args[0])
            case "evaluate":
              page = await self.ensure_page()
              await page.evaluate(args[0])
            case "evaluate_handle":
              page = await self.ensure_page()
              await page.evaluate_handle(args[0])
            case "select":
              page = await self.ensure_page()
              await page.select_option(args[0], args[1])
            case "click":
              locator = await self.ensure_selected(args[0] if args else None)
              await locator.click()
            case "wait":
              await sleep(int(args[0]))
            case "wait_random":
              await sleep(random.uniform(int(args[0]), int(args[1])))
            case "wait_for_selector":
              page = await self.ensure_page()
              await page.wait_for_selector(args[0])
            case "wait_for_url":
              page = await self.ensure_page()
              await page.wait_for_url(args[0])
            case "wait_for_load_state":
              page = await self.ensure_page()
              await page.wait_for_load_state(args[0] if args else "load")
            case "screenshot":
              page = await self.ensure_page()
              await page.screenshot(path=args[0] if args else "screenshot.png")
            case "pdf":
              page = await self.ensure_page()
              await page.pdf(path=args[0] if args else "page.pdf")
            case _:
              log_error(f"Unknown page action: {action}")

        case "select":
          await self.select(args[0], by=args[1] if len(args) > 1 else "auto")

        case "element":
          match command:
            case "click":
              await (await self.ensure_selected()).click()
            case "fill":
              await (await self.ensure_selected()).fill(args[0])
            case "type":
              await (await self.ensure_selected()).type(args[0])
            case "check":
              await (await self.ensure_selected()).check()
            case "uncheck":
              await (await self.ensure_selected()).uncheck()
            case "hover":
              await (await self.ensure_selected()).hover()
            case "focus":
              await (await self.ensure_selected()).focus()
            case "clear":
              await (await self.ensure_selected()).clear()
            case "scroll_into_view":
              await (await self.ensure_selected()).scroll_into_view_if_needed()
            case _:
              log_error(f"Unknown element action: {action}")

        case _:
          log_error(f"Unknown action part: {part}")

    except Exception as e:
      log_error(f"Error executing action {action}: {e}")
      raise


async def update_page(page: Page, action: str, selector: str, *args) -> Page:
  """Update a page with a given action on a selector."""

  part, command = action.split(":")
  try:
    match part:
      case "click":
        locator = page.locator(selector)
        await locator.click()
      case "fill":
        locator = page.locator(selector)
        await locator.fill(args[0] if args else "")
      case "select":
        locator = page.locator(selector)
        await locator.select_option(args[0] if args else "")
      case "check":
        locator = page.locator(selector)
        await locator.check()
      case "uncheck":
        locator = page.locator(selector)
        await locator.uncheck()
      case "hover":
        locator = page.locator(selector)
        await locator.hover()
      case "type":
        locator = page.locator(selector)
        await locator.type(args[0] if args else "")
      case "wait":
        await page.wait_for_selector(selector)
      case "goto":
        await page.goto(selector)  # selector is URL in this case
      case _:
        log_error(f"Unknown page action: {action}")
  except Exception as e:
    log_error(f"Error updating page with action {action}: {e}")
    raise

  return page


async def schedule(ing: Ingester) -> list[Task]:
  """Schedule dynamic scrapping tasks."""

  async def ingest(ing: Ingester):
    """Ingest data using dynamic scrapping."""
    await ing.pre_ingest()

    try:
      if async_playwright is None:
        log_error("Playwright async_playwright not available")
        return

      async with async_playwright() as p:
        puppets = await gather(*[
            Puppet.from_field(field, ing, p) for field in ing.fields
            if field.target  # Check if field has a target URL
        ])

        # Extract data from each puppet and update field values
        for puppet in puppets:
          try:
            contents = await puppet.ensure_contents()
            if contents:
              # Set field value to the extracted content
              if len(contents) == 1:
                puppet.field.value = contents[0]
              else:
                puppet.field.value = contents
          except Exception as e:
            log_error(
                f"Error extracting content from {puppet.field.name}: {e}")

        # Clean up
        await gather(*[puppet.kill() for puppet in puppets])

    except Exception as e:
      log_error(f"Error in dynamic scrapper ingest: {e}")
      raise

    await ing.post_ingest()

  # globally register/schedule the ingester
  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
