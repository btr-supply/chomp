from asyncio import Task, gather, sleep
import importlib.util
from os import path
from typing import Callable, Union, Any

from ..actions.schedule import scheduler
from ..cache import get_cache

from ..utils import log_debug, log_error, log_warn, safe_eval
from .. import state
from ..models.ingesters import Ingester


async def load_handler(
    handler_path: Union[str, Callable[..., Any]]) -> Callable:
  """Load handler function from an external module."""
  try:
    # If already a callable, return it directly
    if callable(handler_path):
      return handler_path

    if handler_path.endswith(".py"):
      # Convert relative path to absolute path
      abs_path = path.abspath(handler_path)

      # Load module from the file path
      module_name = path.splitext(
          path.basename(abs_path))[0]  # e.g., "test" from "test.py"
      spec = importlib.util.spec_from_file_location(module_name, abs_path)

      if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        module.__package__ = "chomp.src.processors"
        spec.loader.exec_module(module)
        return getattr(
            module,
            "handler")  # expects a "handler" function exists in the module
      else:
        raise ImportError(f"Could not load spec from {abs_path}")
    else:
      # Support inline handlers (e.g., dynamically evaluated code)
      return safe_eval(handler_path, callable_check=True)
  except Exception as e:
    log_error(f"Failed to load handler {handler_path}: {e}")
    raise


async def schedule(ing: "Ingester") -> list[Task]:
  """Schedule processor ingester"""

  async def ingest(ing: "Ingester"):
    await ing.pre_ingest()

    # Wait for dependencies to be processed (half the interval)
    wait_time = ing.interval_sec // 2
    if wait_time > 0:
      if state.args.verbose:
        log_debug(f"Waiting {wait_time}s for dependencies to be processed...")
      await sleep(wait_time)

    # Load handler if specified
    handler = None
    if hasattr(ing, 'handler'):
      handler = await load_handler(ing.handler)

    # Get dependency data
    inputs = {}
    deps = ing.dependencies()
    cache_tasks = [get_cache(dep, pickled=True) for dep in deps]

    # Handle empty cache_tasks properly
    if cache_tasks:
      sync_caches = await gather(*cache_tasks)
      inputs = dict(zip(deps, sync_caches))
    else:
      sync_caches = []
      inputs = {}

    if not any(inputs.values()):
      log_warn(f"No dependency data available for {ing.name}")

    # Process data through handler
    try:
      if handler:
        results = handler(ing, inputs)
      else:
        # If no handler specified, just copy selected fields
        results = {}
        for field in ing.fields:
          if field.selector and '.' in field.selector:
            ingester_name, field_name = field.selector.split('.', 1)
            if ingester_name in inputs:
              results[field.name] = inputs[ingester_name].get(field_name)

      # Update field values
      for field in ing.fields:
        if field.name in results:
          field.value = results[field.name]
        elif not field.selector:  # Warn about missing computed fields
          log_warn(f"Handler did not return value for field {field.name}")

      # Store results
      await ing.post_ingest(response_data=results)

    except Exception as e:
      log_error(f"Failed to process {ing.name}: {e}")

  # Register/schedule the ingester
  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
