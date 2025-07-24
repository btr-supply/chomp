from asyncio import Task, gather
from typing import Any, Optional, Dict

# Local application imports are first to ensure patches are applied
# before any third-party libraries that need them are imported.
from ..models import Ingester, ResourceField
from ..utils import log_error, log_warn, log_debug
from ..actions import scheduler
from .. import state

# Now, import the third-party library that has been patched.
from ..adapters.evm_multicall import Call, Multicall


async def schedule(ing: Ingester) -> list[Task]:

  async def ingest(ing: Ingester):
    await ing.pre_ingest()

    # Group calls by chain
    calls_by_chain: Dict[int, Multicall] = {}
    unique_calls = set()

    for field in ing.fields:
      if not field.target or field.target_id in unique_calls:
        if field.target_id in unique_calls:
          log_warn(
              f"Duplicate target {field.target} in {ing.name}.{field.name}, skipping..."
          )
        continue
      unique_calls.add(field.target_id)

      chain_id, addr = field.chain_addr()
      if chain_id not in calls_by_chain:
        client = await state.web3.client(chain_id, roll=True)
        calls_by_chain[chain_id] = Multicall(calls=[],
                                             w3=client,
                                             require_success=False)

      returns = [(f"{field.name}:{i}", lambda success, value: value
                  if success else None)
                 for i in range(len(field.selector_outputs))]
      calls_by_chain[chain_id].calls.append(
          Call(target=addr,
               function=[field.selector, *field.params],
               returns=returns))

    async def execute_multicall(m: Multicall):
      # Let Web3Proxy handle RPC rotation and Multicall handle rebatching
      return await m

    # Execute all chains concurrently
    tasks = [execute_multicall(m) for m in calls_by_chain.values()]
    all_results: Dict[str, Any] = {}

    try:
      outputs = await gather(*tasks, return_exceptions=True)

      # Flatten and process multicall results
      call_index = 0
      for result_batch in outputs:
        if isinstance(result_batch, Exception):
          log_error(f"Error in multicall execution: {result_batch}")
          result_batch = None
        if not result_batch:
          continue

        if isinstance(result_batch, list):
          for result in result_batch:
            if isinstance(result, dict):
              all_results.update(result)
            call_index += 1

      if state.args.verbose:
        log_debug(f"Multicall output for {ing.name}: {all_results}")

      # Update fields directly with values from multicall results
      for field_name, values in all_results.items():
        target_field: Optional[ResourceField] = ing.get_field(
            field_name.split(':')[0])
        if target_field is not None:
          target_field.value = values[0] if len(values) == 1 else values

    except Exception as e:
      log_error(f"Error processing multicalls for {ing.name}: {e}")

    await ing.post_ingest(response_data=all_results)

  # globally register/schedule the ingester
  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
