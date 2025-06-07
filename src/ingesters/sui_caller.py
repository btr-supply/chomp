# TODO: implement using https://docs.sui.io/sui-api-ref + https://suiscan.xyz/

from asyncio import Task
from typing import Any, Dict
from ..model import Ingester, ResourceField
from ..utils import log_debug, log_error
from ..actions import transform_and_store, scheduler
from ..cache import ensure_claim_task
from .. import state
from ..adapters.sui_rpc import SuiRpcClient

max_batch_size = 50  # Using a conservative batch size for Sui


def parse_generic(data: Any) -> Any:
  return data


async def schedule(c: Ingester) -> list[Task]:

  async def ingest(c: Ingester):
    await ensure_claim_task(c)
    data_by_object = {}

    async def fetch_objects_batch(fields: list[ResourceField],
                                  max_retries: int = state.args.max_retries,
                                  max_batch_size=50) -> Dict[str, Any]:
      retry_count = 0
      results: Dict[str, Any] = {}

      # Get unique object IDs while preserving order
      unique_objects = list({f.target for f in fields if f.target})

      # Split objects into batches
      for i in range(0, len(unique_objects), max_batch_size):
        batch = unique_objects[i:i + max_batch_size]

        while retry_count < max_retries:
          client = await state.web3.client("sui", roll=True)
          try:
            # Type guard: ensure we have a SuiRpcClient
            if not isinstance(client, SuiRpcClient):
              raise TypeError(f"Expected SuiRpcClient, got {type(client)}")

            if state.args.verbose:
              log_debug(
                  f"Fetching batch {i//max_batch_size + 1} of {(len(unique_objects) + max_batch_size - 1)//max_batch_size}"
              )

            # Create a mapping of index to object ID for this batch
            batch_index_map = {i: obj_id for i, obj_id in enumerate(batch)}

            # Fetch objects for this batch
            object_data = await client.get_multi_object_fields(batch)

            # Process results using the index mapping
            for i, obj in enumerate(object_data):
              if obj is None:
                continue

              object_id = batch_index_map[i]
              data_by_object[object_id] = object_data[i]

            break  # Success, move to next batch

          except Exception as e:
            log_error(
                f"Error fetching objects batch {i//max_batch_size + 1}, switching RPC... ({str(e)})"
            )
            # Type guard for endpoint access
            if isinstance(client, SuiRpcClient):
              prev_rpc = client.endpoint
            else:
              prev_rpc = "unknown"
            client = await state.web3.client("sui", roll=True)
            if isinstance(client, SuiRpcClient):
              new_rpc = client.endpoint
            else:
              new_rpc = "unknown"
            if state.args.verbose:
              log_debug(f"Switched RPC {prev_rpc} -> {new_rpc}")
            retry_count += 1

        if retry_count >= max_retries:
          log_error(
              f"Failed to fetch objects batch after {max_retries} retries")
          return results

      # Process results for all fields after all batches are fetched
      for field in fields:
        if field.target not in data_by_object:
          log_error(f"Object not found: {field.target}")
          continue

        # TODO: handle comma separatted multi selectors cf. svm_caller.py
        if field.selector and data_by_object[field.target]:
          data = data_by_object[field.target]
          results[field.target] = [data[field.selector]]

      return results

    # Fetch all objects
    batch_results = await fetch_objects_batch(c.fields)

    # Process results for each field
    for field in c.fields:
      try:
        if field.target in batch_results:
          field.value = batch_results[field.target]
          c.data_by_field[field.name] = field.value
      except Exception as e:
        log_error(f"Error processing {field.name}: {str(e)}")

    if state.args.verbose:
      log_debug(f"Ingested {c.name} -> {c.data_by_field}")

    await transform_and_store(c)

  # Handle Optional[Task] return from scheduler.add_ingester
  task = await scheduler.add_ingester(c, fn=ingest, start=False)
  return [task] if task is not None else []
