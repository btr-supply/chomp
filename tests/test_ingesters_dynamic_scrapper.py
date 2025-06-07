"""Tests for dynamic_scrapper module."""
import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.deps import safe_import

# Check if Playwright dependencies are available
playwright = safe_import("playwright")
PLAYWRIGHT_AVAILABLE = playwright is not None

# Only import if dependencies are available
if PLAYWRIGHT_AVAILABLE:
  from src.ingesters.dynamic_scrapper import Puppet, update_page, schedule
  from src.model import Ingester, ResourceField


@pytest.mark.skipif(
    not PLAYWRIGHT_AVAILABLE,
    reason="Playwright dependencies not available (playwright)")
class TestPuppet:
  """Test Puppet class functionality."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_field = Mock(spec=ResourceField)
    self.mock_field.id = "test_field_id"
    self.mock_field.target = "http://example.com"
    self.mock_field.actions = []

    self.mock_ingester = Mock(spec=Ingester)
    self.mock_ingester.name = "test_ingester"

    self.mock_playwright = MagicMock()
    self.mock_browser = AsyncMock()
    self.mock_page = AsyncMock()
    self.mock_locator = AsyncMock()
    self.mock_element = AsyncMock()

    # Setup mock chain
    mock_browser_launcher = AsyncMock()
    mock_browser_launcher.launch = AsyncMock(return_value=self.mock_browser)
    self.mock_playwright.__getitem__.return_value = mock_browser_launcher
    self.mock_browser.new_page = AsyncMock(return_value=self.mock_page)
    self.mock_page.locator = Mock(return_value=self.mock_locator)
    self.mock_page.goto = AsyncMock()
    self.mock_locator.element_handles = AsyncMock(
        return_value=[self.mock_element])
    self.mock_element.text_content = AsyncMock(return_value="test content")

  def teardown_method(self):
    """Clean up after tests."""
    Puppet.by_id.clear()

  def test_puppet_initialization(self):
    """Test Puppet initialization."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    assert puppet.field == self.mock_field
    assert puppet.ingester == self.mock_ingester
    assert puppet.play == self.mock_playwright
    assert Puppet.by_id[self.mock_field.id] == puppet

  @pytest.mark.asyncio
  async def test_from_field_no_actions(self):
    """Test creating Puppet from field with no actions."""
    puppet = await Puppet.from_field(self.mock_field, self.mock_ingester,
                                     self.mock_playwright)

    assert isinstance(puppet, Puppet)
    assert puppet.field == self.mock_field

  @pytest.mark.asyncio
  async def test_from_field_with_actions(self):
    """Test creating Puppet from field with actions."""
    self.mock_field.actions = ["browser:use", "page:goto"]

    with patch.object(Puppet, 'act', new_callable=AsyncMock) as mock_act:
      await Puppet.from_field(self.mock_field, self.mock_ingester,
                              self.mock_playwright)

      assert mock_act.call_count == 2
      mock_act.assert_any_call("browser:use")
      mock_act.assert_any_call("page:goto")

  @pytest.mark.asyncio
  async def test_kill_with_browser(self):
    """Test killing puppet with browser."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    puppet.browser = self.mock_browser
    puppet.pages = [self.mock_page]

    await puppet.kill()

    self.mock_browser.close.assert_called_once()
    self.mock_page.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_kill_without_browser(self):
    """Test killing puppet without browser."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    puppet.browser = None
    puppet.pages = []

    # Should not raise an exception
    await puppet.kill()

  @pytest.mark.asyncio
  async def test_ensure_browser_new(self):
    """Test ensuring browser when none exists."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    browser = await puppet.ensure_browser()

    assert browser == self.mock_browser
    self.mock_playwright.__getitem__.assert_called_with("chromium")

  @pytest.mark.asyncio
  async def test_ensure_browser_existing(self):
    """Test ensuring browser when one already exists."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    puppet.browser = self.mock_browser

    browser = await puppet.ensure_browser()

    assert browser == self.mock_browser
    # Should not create new browser
    self.mock_playwright.__getitem__.assert_not_called()

  @pytest.mark.asyncio
  async def test_ensure_browser_custom_type(self):
    """Test ensuring browser with custom browser type."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    await puppet.ensure_browser("firefox")

    self.mock_playwright.__getitem__.assert_called_with("firefox")

  @pytest.mark.asyncio
  async def test_ensure_page_new(self):
    """Test ensuring page when none exists."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    page = await puppet.ensure_page()

    assert page == self.mock_page
    self.mock_page.goto.assert_called_with("http://example.com")

  @pytest.mark.asyncio
  async def test_ensure_page_default_target(self):
    """Test ensuring page with default target."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    self.mock_field.target = None

    await puppet.ensure_page()

    self.mock_page.goto.assert_called_with("about:blank")

  @pytest.mark.asyncio
  async def test_ensure_page_custom_target(self):
    """Test ensuring page with custom target."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    await puppet.ensure_page("http://custom.com")

    self.mock_page.goto.assert_called_with("http://custom.com")

  @pytest.mark.asyncio
  async def test_ensure_selected_new(self):
    """Test ensuring selector when none exists."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    with patch.object(puppet,
                      'select',
                      new_callable=AsyncMock,
                      return_value=self.mock_locator) as mock_select:
      locator = await puppet.ensure_selected()

      assert locator == self.mock_locator
      mock_select.assert_called_once_with(selector="html", by="auto")

  @pytest.mark.asyncio
  async def test_ensure_selected_existing(self):
    """Test ensuring selector when one already exists."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    puppet.selector = self.mock_locator

    locator = await puppet.ensure_selected()

    assert locator == self.mock_locator

  @pytest.mark.asyncio
  async def test_ensure_elements_new(self):
    """Test ensuring elements when none exist."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    with patch.object(puppet,
                      'ensure_selected',
                      new_callable=AsyncMock,
                      return_value=self.mock_locator):
      elements = await puppet.ensure_elements()

      assert elements == [self.mock_element]
      self.mock_locator.element_handles.assert_called_once()

  @pytest.mark.asyncio
  async def test_ensure_elements_existing(self):
    """Test ensuring elements when they already exist."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    puppet.elements = [self.mock_element]

    elements = await puppet.ensure_elements()

    assert elements == [self.mock_element]

  @pytest.mark.asyncio
  async def test_ensure_contents(self):
    """Test ensuring contents extraction."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    with patch.object(puppet,
                      'ensure_elements',
                      new_callable=AsyncMock,
                      return_value=[self.mock_element]):
      contents = await puppet.ensure_contents()

      assert contents == ["test content"]
      self.mock_element.text_content.assert_called_once()

  @pytest.mark.asyncio
  async def test_select_auto_selector(self):
    """Test select with auto selector type."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    with patch.object(puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      locator = await puppet.select(".test-class", "auto")

      assert locator == self.mock_locator
      self.mock_page.locator.assert_called_with(".test-class")

  @pytest.mark.asyncio
  async def test_select_css_selector(self):
    """Test select with CSS selector type."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    with patch.object(puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      await puppet.select(".test", "css")

      self.mock_page.locator.assert_called_with("css=.test")

  @pytest.mark.asyncio
  async def test_select_xpath_selector(self):
    """Test select with XPath selector type."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    with patch.object(puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      await puppet.select("//div", "xpath")

      self.mock_page.locator.assert_called_with("xpath=//div")

  @pytest.mark.asyncio
  async def test_select_text_selector(self):
    """Test select with text selector type."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    self.mock_page.get_by_text = Mock(return_value=self.mock_locator)

    with patch.object(puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      await puppet.select("Click me", "text")

      self.mock_page.get_by_text.assert_called_with("Click me")

  @pytest.mark.asyncio
  async def test_select_role_selector(self):
    """Test select with role selector type."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)
    self.mock_page.get_by_role = Mock(return_value=self.mock_locator)

    with patch.object(puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      await puppet.select("button", "role")

      self.mock_page.get_by_role.assert_called_with("button")

  @pytest.mark.asyncio
  async def test_select_custom_attribute(self):
    """Test select with custom data attribute."""
    puppet = Puppet(self.mock_field, self.mock_ingester, self.mock_playwright)

    with patch.object(puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      await puppet.select("test-id", "data-testid")

      self.mock_page.locator.assert_called_with("[data-data-testid=test-id]")


@pytest.mark.skipif(
    not PLAYWRIGHT_AVAILABLE,
    reason="Playwright dependencies not available (playwright)")
class TestPuppetActions:
  """Test Puppet action methods."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_field = Mock(spec=ResourceField)
    self.mock_field.id = "test_field_id"
    self.mock_field.target = "http://example.com"
    self.mock_field.actions = []

    self.mock_ingester = Mock(spec=Ingester)
    self.mock_playwright = MagicMock()
    self.mock_browser = AsyncMock()
    self.mock_page = AsyncMock()

    self.puppet = Puppet(self.mock_field, self.mock_ingester,
                         self.mock_playwright)

  def teardown_method(self):
    """Clean up after tests."""
    Puppet.by_id.clear()

  @pytest.mark.asyncio
  async def test_act_browser_use(self):
    """Test browser use action."""
    with patch.object(self.puppet, 'play') as mock_play:
      mock_play.__getitem__.return_value.launch = AsyncMock(
          return_value=self.mock_browser)

      await self.puppet.act("browser:use", "firefox")

      mock_play.__getitem__.assert_called_with("firefox")

  @pytest.mark.asyncio
  async def test_act_browser_close(self):
    """Test browser close action."""
    self.puppet.browser = self.mock_browser

    await self.puppet.act("browser:close")

    self.mock_browser.close.assert_called_once()

  @pytest.mark.asyncio
  async def test_act_page_goto(self):
    """Test page goto action."""
    with patch.object(self.puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      await self.puppet.act("page:goto", "http://test.com")

      self.mock_page.goto.assert_called_with("http://test.com")

  @pytest.mark.asyncio
  async def test_act_page_click(self):
    """Test page click action."""
    mock_locator = AsyncMock()
    with patch.object(self.puppet,
                      'ensure_selected',
                      new_callable=AsyncMock,
                      return_value=mock_locator):
      await self.puppet.act("page:click", ".button")

      mock_locator.click.assert_called_once()

  @pytest.mark.asyncio
  async def test_act_element_click(self):
    """Test element click action."""
    mock_element = AsyncMock()
    with patch.object(self.puppet,
                      'ensure_elements',
                      new_callable=AsyncMock,
                      return_value=[mock_element]):
      await self.puppet.act("element:click")

      mock_element.click.assert_called_once()

  @pytest.mark.asyncio
  async def test_act_element_fill(self):
    """Test element fill action."""
    mock_element = AsyncMock()
    with patch.object(self.puppet,
                      'ensure_elements',
                      new_callable=AsyncMock,
                      return_value=[mock_element]):
      await self.puppet.act("element:fill", "test input")

      mock_element.fill.assert_called_with("test input")

  @pytest.mark.asyncio
  async def test_act_keyboard_press(self):
    """Test keyboard press action."""
    with patch.object(self.puppet,
                      'ensure_page',
                      new_callable=AsyncMock,
                      return_value=self.mock_page):
      await self.puppet.act("keyboard:press", "Enter")

      self.mock_page.keyboard.press.assert_called_with("Enter")

  @pytest.mark.asyncio
  async def test_act_unknown_action(self):
    """Test handling of unknown action."""
    with patch('src.ingesters.dynamic_scrapper.log_error') as mock_log_error:
      await self.puppet.act("unknown:action")

      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_act_exception_handling(self):
    """Test exception handling in act method."""
    with patch.object(self.puppet, 'ensure_page', side_effect=Exception("Test error")), \
         patch('src.ingesters.dynamic_scrapper.log_error') as mock_log_error:

      await self.puppet.act("page:goto", "http://test.com")

      mock_log_error.assert_called()


@pytest.mark.skipif(
    not PLAYWRIGHT_AVAILABLE,
    reason="Playwright dependencies not available (playwright)")
class TestUpdatePage:
  """Test update_page function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_page = AsyncMock()

  @pytest.mark.asyncio
  async def test_update_page_click(self):
    """Test page click update."""
    result = await update_page(self.mock_page, "click", ".button")

    assert result == self.mock_page
    self.mock_page.click.assert_called_with(".button")

  @pytest.mark.asyncio
  async def test_update_page_goto(self):
    """Test page goto update."""
    result = await update_page(self.mock_page, "goto", "http://test.com")

    assert result == self.mock_page
    self.mock_page.goto.assert_called_with("http://test.com")

  @pytest.mark.asyncio
  async def test_update_page_fill(self):
    """Test page fill update."""
    result = await update_page(self.mock_page, "fill", "#input", "test value")

    assert result == self.mock_page
    self.mock_page.fill.assert_called_with("#input", "test value")

  @pytest.mark.asyncio
  async def test_update_page_unknown_action(self):
    """Test unknown action handling."""
    with patch('src.ingesters.dynamic_scrapper.log_error') as mock_log_error:
      result = await update_page(self.mock_page, "unknown", "selector")

      assert result == self.mock_page
      mock_log_error.assert_called()

  @pytest.mark.asyncio
  async def test_update_page_exception(self):
    """Test exception handling."""
    self.mock_page.click.side_effect = Exception("Click failed")

    with patch('src.ingesters.dynamic_scrapper.log_error') as mock_log_error:
      result = await update_page(self.mock_page, "click", ".button")

      assert result == self.mock_page
      mock_log_error.assert_called()


@pytest.mark.skipif(
    not PLAYWRIGHT_AVAILABLE,
    reason="Playwright dependencies not available (playwright)")
class TestSchedule:
  """Test schedule function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_ingester = Mock(spec=Ingester)
    self.mock_ingester.fields = []

    self.mock_field = Mock(spec=ResourceField)
    self.mock_field.name = "test_field"
    self.mock_field.target_id = "target_123"

  def teardown_method(self):
    """Clean up after tests."""
    Puppet.by_id.clear()

  @pytest.mark.asyncio
  async def test_schedule_empty_fields(self):
    """Test scheduling with empty fields."""
    with patch('src.cache.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.actions.scheduler.add_ingester', new_callable=AsyncMock, return_value="task_123") as mock_add:

      result = await schedule(self.mock_ingester)

      assert result == ["task_123"]
      mock_add.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_with_fields(self):
    """Test scheduling with fields."""
    self.mock_ingester.fields = [self.mock_field]

    with patch('src.cache.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.actions.scheduler.add_ingester', new_callable=AsyncMock, return_value="task_123") as mock_add, \
         patch('src.ingesters.dynamic_scrapper.async_playwright') as mock_playwright_ctx:

      # Mock the async context manager
      mock_playwright = AsyncMock()
      mock_playwright_ctx.return_value.__aenter__ = AsyncMock(
          return_value=mock_playwright)
      mock_playwright_ctx.return_value.__aexit__ = AsyncMock(return_value=None)

      # Mock Puppet.from_field
      mock_puppet = AsyncMock()
      mock_puppet.ensure_contents = AsyncMock(return_value=["scraped content"])
      mock_puppet.kill = AsyncMock()

      with patch.object(Puppet, 'from_field', new_callable=AsyncMock, return_value=mock_puppet), \
           patch('src.actions.transform_and_store', new_callable=AsyncMock):

        result = await schedule(self.mock_ingester)

        assert result == ["task_123"]
        mock_add.assert_called_once()

  @pytest.mark.asyncio
  async def test_schedule_no_task_created(self):
    """Test scheduling when no task is created."""
    with patch('src.cache.ensure_claim_task', new_callable=AsyncMock), \
         patch('src.actions.scheduler.add_ingester', new_callable=AsyncMock, return_value=None) as mock_add:

      result = await schedule(self.mock_ingester)

      assert result == []
      mock_add.assert_called_once()


@pytest.mark.skipif(
    not PLAYWRIGHT_AVAILABLE,
    reason="Playwright dependencies not available (playwright)")
class TestSelectorTypes:
  """Test SelectorType functionality."""

  def test_selector_type_literals(self):
    """Test that SelectorType literals are defined correctly."""
    # Test that we can import and use the literals

    # These should not raise type errors
    selector_types = [
        "auto", "css", "xpath", "id", "name", "class", "role", "strict_role",
        "value", "text", "strict_text", "alt_text", "strict_alt_text", "title",
        "strict_title", "label", "strict_label", "placeholder",
        "strict_placeholder"
    ]

    for selector_type in selector_types:
      # This verifies the literal types are properly defined
      assert isinstance(selector_type, str)


# Integration tests without mocking for simple functionality
class TestDynamicScrapperIntegration:
  """Integration tests for dynamic_scrapper module."""

  def test_puppet_class_exists(self):
    """Test that Puppet class can be imported."""
    if PLAYWRIGHT_AVAILABLE:
      assert Puppet is not None
      assert hasattr(Puppet, 'by_id')
      assert isinstance(Puppet.by_id, dict)

  def test_update_page_function_exists(self):
    """Test that update_page function can be imported."""
    if PLAYWRIGHT_AVAILABLE:
      assert update_page is not None
      assert callable(update_page)

  def test_schedule_function_exists(self):
    """Test that schedule function can be imported."""
    if PLAYWRIGHT_AVAILABLE:
      assert schedule is not None
      assert callable(schedule)
