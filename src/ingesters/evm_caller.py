from asyncio import Task, TimeoutError
from multicall import Call, Multicall, constants as mc_const
import asyncio
from typing import Any, Dict, Optional

from ..model import Ingester
from ..utils import log_debug, log_error, log_warn
from ..actions import transform_and_store, scheduler
from ..cache import ensure_claim_task
from .. import state

def parse_generic(data: Any) -> Any:
  return data

async def schedule(c: Ingester) -> list[Task]:

  async def ingest(c: Ingester):
    await ensure_claim_task(c)
    unique_calls, calls_by_chain = set(), {}
    field_by_name = c.field_by_name(include_transient=True)

    for field in c.fields:
      if not field.target or field.id in unique_calls:
        if field.id in unique_calls:
          log_warn(f"Duplicate target smart contract view {field.target} in {c.name}.{field.name}, skipping...")
        continue
      unique_calls.add(field.id)

      chain_id, addr = field.chain_addr()
      if chain_id not in calls_by_chain:
        client = await state.web3.client(chain_id, roll=True)  # Keep as Any type for now
        calls_by_chain[chain_id] = Multicall(calls=[], _w3=client, require_success=True, gas_limit=mc_const.GAS_LIMIT)
      returns = [[f"{field.name}:{i}", parse_generic] for i in range(len(field.selector_outputs))]
      calls_by_chain[chain_id].calls.append(Call(target=addr, function=[field.selector, *field.params], returns=returns))

    tp = state.thread_pool

    async def execute_multicall(m: Multicall, max_retries: int = state.args.max_retries):
      output = None
      retry_count = 0
      while not output and retry_count < max_retries:
        try:
          output = tp.submit(m).result(timeout=3)
          return output
        except Exception as e:
          log_error(f"Multicall for chain {m.w3.eth.chain_id} failed: {e}, switching RPC...")
          prev_rpc = m.w3.provider.endpoint_uri
          m.w3 = await state.web3.client(m.w3.eth.chain_id, roll=True)  # Keep as Any type for now
          new_rpc = m.w3.provider.endpoint_uri
          if state.args.verbose:
            log_debug(f"Switched RPC {prev_rpc} -> {new_rpc}")
          retry_count += 1
      return None

    async def call_multi(m: Multicall, retry_batch_size=4, split_on_failure=False, max_batch_size=100):
      try:
        # Split into initial batches if calls exceed max_batch_size
        if len(m.calls) > max_batch_size:
          if state.args.verbose:
            log_debug(f"Calls exceed max_batch_size ({len(m.calls)} > {max_batch_size}), splitting into batches...")
          all_results = {}

          for i in range(0, len(m.calls), max_batch_size):
            batch = m.calls[i:i + max_batch_size]
            batch_multi = Multicall(
              calls=batch,
              _w3=m.w3,
              require_success=True,
              gas_limit=mc_const.GAS_LIMIT
            )
            try:
              batch_output = await execute_multicall(batch_multi)
              if batch_output:
                all_results.update(batch_output)
              else:
                # If initial batch fails, try with smaller retry batches
                sub_results = await handle_failed_batch(batch_multi, retry_batch_size)
                all_results.update(sub_results)
            except Exception as e:
              log_warn(f"Batch {i//max_batch_size + 1} failed ({str(e)}), trying smaller batches...")
              sub_results = await handle_failed_batch(batch_multi, retry_batch_size)
              all_results.update(sub_results)

          return all_results

        # For calls within max_batch_size, try full multicall first
        output = await execute_multicall(m)
        if output:
          return output

        if not split_on_failure:
          log_error(f"Multicall failed for chain {m.w3.eth.chain_id}, skipping...")
          return {}

        # Initial call failed, try with smaller batches
        return await handle_failed_batch(m, retry_batch_size)

      except Exception as e:
        log_error(f"Failed to execute multicall for chain {m.w3.eth.chain_id}: {e}")
        return {}

    async def handle_failed_batch(m: Multicall, retry_batch_size: int) -> dict:
      """Helper function to handle retrying failed batches with smaller sizes"""
      log_warn(f"Multicall failed, splitting into batches of {retry_batch_size} and processing concurrently...")
      all_results = {}

      for i in range(0, len(m.calls), retry_batch_size):
        batch = m.calls[i:i + retry_batch_size]
        batch_multi = Multicall(
          calls=batch,
          _w3=m.w3,
          require_success=True,
          gas_limit=mc_const.GAS_LIMIT
        )
        try:
          batch_output = await execute_multicall(batch_multi)
          if batch_output:
            all_results.update(batch_output)
        except (TimeoutError, Exception) as e:
          log_warn(f"Batch {i + 1} failed ({str(e)}), trying individual calls...")

          for call in batch:
            single_multi = Multicall(
              calls=[call],
              _w3=m.w3,
              require_success=True,
              gas_limit=mc_const.GAS_LIMIT
            )
            try:
              single_output = await execute_multicall(single_multi)
              if single_output:
                all_results.update(single_output)
            except (TimeoutError, Exception) as e:
              log_error(f"Individual call failed - Target: {call.target}, Function: {call.function[0]}, Error: {str(e)}")

      return all_results

    futures = []
    for chain_id, m in calls_by_chain.items():
      futures.append(call_multi(m))

    try:
      outputs = await asyncio.gather(*futures, return_exceptions=True)

      for output in outputs:
        if isinstance(output, Exception):
          log_error(f"Error in multicall execution: {output}")
          continue

        # Skip if output is not a dict-like object
        if not isinstance(output, dict):
          log_error(f"Unexpected output type: {type(output)}")
          continue

        # Group values by field name (stripping array index)
        field_values: Dict[str, Any] = {}
        for key, value in output.items():
          field_name = key.split(':')[0]
          index = int(key.split(':')[1])
          if field_name not in field_values:
            field_values[field_name] = []
          # Extend list if needed to accommodate index
          while len(field_values[field_name]) <= index:
            field_values[field_name].append(None)
          # Handle tuple values by taking first element
          field_values[field_name][index] = value

        # Update fields with grouped values
        for name, values in field_values.items():
          target_field: Optional[Any] = field_by_name.get(name)
          if target_field is not None:
            # If only one value, store directly rather than as list
            target_field.value = values[0] if len(values) == 1 else values
            c.data_by_field[target_field.name] = target_field.value

    except Exception as e:
      log_error(f"Error processing parallel multicalls: {e}")

    if state.args.verbose:
      log_debug(f"Ingested {c.name} -> {c.data_by_field}")

    await transform_and_store(c)

  # globally register/schedule the ingester
  task = await scheduler.add_ingester(c, fn=ingest, start=False)
  return [task] if task is not None else []
