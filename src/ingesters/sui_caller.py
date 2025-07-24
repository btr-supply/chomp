# TODO: implement using https://docs.sui.io/sui-api-ref + https://suiscan.xyz/

from asyncio import Task
from typing import Any
from ..models import Ingester, ResourceField
from ..utils import log_debug, log_error
from ..actions import scheduler
from .. import state
from ..adapters.sui_rpc import SuiRpcClient


async def schedule(ing: Ingester) -> list[Task]:

  async def ingest(ing: Ingester):
    await ing.pre_ingest()
    data_by_object = {}

    async def fetch_objects_batch(
        fields: list[ResourceField]) -> dict[str, Any]:
      results: dict[str, Any] = {}
      unique_objects = list({f.target for f in fields if f.target})
      max_batch_size = 50  # Conservative batch size for Sui

      # Process objects in batches
      for i in range(0, len(unique_objects), max_batch_size):
        batch = unique_objects[i:i + max_batch_size]

        try:
          client = await state.web3.client("sui", roll=True)
          if not isinstance(client, SuiRpcClient):
            raise TypeError(f"Expected SuiRpcClient, got {type(client)}")

          if state.args.verbose:
            batch_num = i // max_batch_size + 1
            total_batches = (len(unique_objects) + max_batch_size -
                             1) // max_batch_size
            log_debug(
                f"Fetching batch {batch_num}/{total_batches} from RPC: {client.endpoint}"
            )

          # Fetch objects for this batch
          object_data = await client.get_multi_object_fields(batch)

          # Process results
          for idx, obj in enumerate(object_data):
            if obj is None:
              continue
            object_id = batch[idx]
            data_by_object[object_id] = object_data[idx]

        except Exception as e:
          log_error(
              f"Error fetching objects batch {i // max_batch_size + 1}: {e}")
          continue

      # Process results for all fields
      for field in fields:
        if field.target not in data_by_object:
          field_names_for_object = [
              f.name for f in fields if f.target == field.target
          ]
          if not field.target:
            log_error(f"Field {field.name} has no target")
            continue

          all_configured_objects = set(f.target for f in ing.fields
                                       if f.target)

          ing.log_resource_not_found(
              resource_type="Object",
              resource_id=field.target,
              field_names=field_names_for_object,
              endpoint=getattr(client, 'endpoint', 'unknown'),
              retries_exhausted=False,
              batch_size=len(unique_objects),
              total_fields=len(ing.fields),
              total_unique_resources=len(all_configured_objects))
          continue

        data = data_by_object[field.target]
        if not field.selector:
          results[field.target] = data
          continue

        # Multi selector parsing: support comma-separated selectors
        selector_parts = field.selector.split(",")
        selected_data: list[Any] = []
        for selector in selector_parts:
          if ":" in selector:
            # Range-based selector (e.g., "0:8" for byte slicing)
            try:
              start, end = map(int, selector.split(":"))
              selected_data.append(data[start:end])
            except (ValueError, TypeError, IndexError) as e:
              log_error(
                  f"Error processing range selector '{selector}' for field {field.name}: {e}"
              )
              continue
          else:
            # Key-based selector (e.g., "balance" for object field access)
            try:
              if isinstance(data, dict):
                selected_data.append(data.get(selector))
              elif hasattr(data, selector):
                selected_data.append(getattr(data, selector))
              else:
                log_error(
                    f"Cannot access selector '{selector}' on data type {type(data)} for field {field.name}"
                )
                continue
            except Exception as e:
              log_error(
                  f"Error processing key selector '{selector}' for field {field.name}: {e}"
              )
              continue
        results[field.target] = selected_data

      return results

    # Fetch all objects and process results
    batch_results = await fetch_objects_batch(ing.fields)
    ing.process_batch_results(batch_results)
    await ing.post_ingest(response_data=batch_results)

  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
