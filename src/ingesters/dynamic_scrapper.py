from asyncio import sleep, gather
import random
from typing import Literal, Any
from playwright.async_api import async_playwright, ViewportSize, Page, Locator, Browser, Playwright, ElementHandle

from ..utils import log_error
from ..model import Ingester, ResourceField
from ..cache import ensure_claim_task
from ..actions import transform_and_store, scheduler

SelectorType = Literal["auto", "css", "xpath", "id", "name", "class", "role",
                       "strict_role", "value", "text", "strict_text",
                       "alt_text", "strict_alt_text", "title", "strict_title",
                       "label", "strict_label", "placeholder",
                       "strict_placeholder"]


class Puppet:
  by_id: dict[str, "Puppet"] = {}

  ingester: Ingester
  field: ResourceField
  play: Playwright
  browser: Browser
  pages: list[Page] = []
  selector: Locator
  elements: list[ElementHandle] = []

  def __init__(self, field: ResourceField, ingester: Ingester,
               play: Playwright):
    Puppet.by_id[field.id] = self
    self.field = field
    self.ingester = ingester
    self.play = play

  @staticmethod
  async def from_field(field: ResourceField, ingester: Ingester,
                       play: Playwright) -> "Puppet":
    p = Puppet(field, ingester, play)
    for a in field.actions:
      await p.act(a)
    return p

  async def kill(self):
    await self.browser.close() if self.browser else None
    await gather(*[p.close() for p in self.pages])

  async def ensure_browser(self, browser="chromium") -> Browser:
    if not self.browser:
      self.browser = await self.play[browser].launch()
    return self.browser

  async def ensure_page(self, target=None) -> Page:
    target = target or self.field.target or "about:blank"
    if not self.pages:
      await self.ensure_browser()
      page = await self.browser.new_page()
      await page.goto(target)
      self.pages = [page]
    return self.pages[-1]

  async def ensure_selected(self, selector="html", by="auto") -> Locator:
    if not hasattr(self, 'selector') or not self.selector:
      await self.ensure_page()
      self.selector = await self.select(selector=selector, by=by)
    return self.selector

  async def ensure_elements(self) -> list[ElementHandle]:
    if not self.elements:
      await self.ensure_selected()
      self.elements = await self.selector.element_handles()
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
        locator = page.get_by_role(selector)  # type: ignore
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
              await self.browser.close() if self.browser else None
            case _:
              log_error(f"Unknown browser action: {action}")

        case "page":
          match command:
            case "new":
              self.pages.append(await self.browser.new_page())
            case "close":
              await self.pages.pop().close() if self.pages else None
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
            case _:
              log_error(f"Unknown page action: {action}")

        case "element":
          match command:
            case "click":
              elements = await self.ensure_elements()
              await elements[0].click()
            case "hover":
              elements = await self.ensure_elements()
              await elements[0].hover()
            case "focus":
              elements = await self.ensure_elements()
              await elements[0].focus()
            case "press":
              elements = await self.ensure_elements()
              await elements[0].press(args[0])
            case "select_option":
              elements = await self.ensure_elements()
              await elements[0].select_option(args[0])  # by value or label
            case "drag_and_drop":
              elements = await self.ensure_elements()
              # ElementHandle doesn't have drag_and_drop, use locator instead
              locator = await self.ensure_selected()
              await locator.drag_to(page.locator(args[0]))
            case "upload":
              elements = await self.ensure_elements()
              await elements[0].set_input_files(args[0])
            case "fill":
              elements = await self.ensure_elements()
              await elements[0].fill(args[0])
            case "type":
              elements = await self.ensure_elements()
              # ElementHandle doesn't have press_sequentially, use locator instead
              locator = await self.ensure_selected()
              await locator.press_sequentially(args[0])
            case "check":
              elements = await self.ensure_elements()
              await elements[0].check()
            case "uncheck":
              elements = await self.ensure_elements()
              await elements[0].uncheck()
            case "scroll_to":
              elements = await self.ensure_elements()
              await elements[0].scroll_into_view_if_needed()
            case _:
              log_error(f"Unknown element action: {action}")

        case "keyboard":
          match command:
            case "press":
              page = await self.ensure_page()
              await page.keyboard.press(args[0])
            case "down":
              page = await self.ensure_page()
              await page.keyboard.down(args[0])
            case "up":
              page = await self.ensure_page()
              await page.keyboard.up(args[0])
            case "type":
              page = await self.ensure_page()
              await page.keyboard.type(args[0])  # can add delay
            case _:
              log_error(f"Unknown keyboard action: {action}")

    except Exception as e:
      log_error(f"Error acting {action}: {e}")


async def update_page(page: Page, action: str, selector: str, *args) -> Page:
  """Executes a state-changing action on the page and returns the modified page."""
  try:
    match action:
      case "click":
        await page.click(selector)
      case "goto":
        await page.goto(selector)
      case "hover":
        await page.hover(selector)
      case "focus":
        await page.focus(selector)
      case "press":
        await page.press(selector, args[0])
      case "select_option":
        await page.select_option(selector, args[0])
      case "wait_for_load_state":
        await page.wait_for_load_state()
      case "wait_for_event":
        await page.wait_for_event(selector)
      case "drag_and_drop":
        await page.drag_and_drop(selector, args[0])
      case "upload":
        await page.set_input_files(selector, args[0])
      case "fill":
        await page.fill(selector, args[0])
      case "wait_for_selector":
        await page.wait_for_selector(selector)
      case "wait_for_timeout":
        await page.wait_for_timeout(int(selector))
      case "scroll_to":  # Added scroll action
        await page.evaluate(f'window.scrollTo({selector}, {args[0]})')
      case "check":  # Added check action
        await page.check(selector)
      case "uncheck":  # Added uncheck action
        await page.uncheck(selector)
      case "mouse":
        if args[0] == "move":
          await page.mouse.move(float(selector), float(args[1]))
        elif args[0] == "down":
          await page.mouse.down()
        elif args[0] == "up":
          await page.mouse.up()

      case "set_viewport_size":
        await page.set_viewport_size(
            ViewportSize(width=int(selector), height=int(args[0])))
      case "emulate_media":
        await page.emulate_media(
            media=args[0])  # Fixed: use emulate_media instead of emulate
      case _:
        log_error(f"Unknown state action: {action}")
  except Exception as e:
    log_error(f"Error executing state action {action}: {e}")

  return page  # Return the modified page


async def schedule(c: Ingester) -> list:
  hashes = {}

  async def ingest(c: Ingester):
    await ensure_claim_task(c)

    hashes.update({f.name: f.target_id for f in c.fields})
    hashes.update({hashes[f.name]: f.name for f in c.fields})

    async with async_playwright() as playwright:
      # futures = action_chains.append(Puppet(hashes[f.name], c, playwright))
      futures = [Puppet.from_field(f, c, playwright) for f in c.fields\
        if Puppet.by_id.get(hashes[f.name]) is None]
      puppets = await gather(*futures)  # run all puppets concurrently
      for i, result in enumerate(puppets):
        contents = await puppets[i].ensure_contents()
        c.fields[i].value = contents[0] if contents else None

      await gather(*[p.kill() for p in puppets])
      await transform_and_store(c)

  task = await scheduler.add_ingester(c, fn=ingest, start=False)
  return [task] if task is not None else []
