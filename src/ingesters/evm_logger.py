from asyncio import Task
from web3 import Web3

from ..models.ingesters import Ingester
from ..utils import log_error, log_info, log_debug, split_chain_addr
from ..actions import scheduler
from .. import state


def parse_event_signature(signature: str) -> tuple[str, list[str], list[bool]]:
  event_name, params = signature.split('(')
  param_list = params.rstrip(')').split(',')
  param_types = [param.split(' ')[-1] for param in param_list]
  indexed = ['indexed' in param for param in param_list]
  return event_name.strip(), param_types, indexed


def decode_log_data(client: Web3, log: dict, topics_first: list[str],
                    indexed: list[bool]) -> tuple:
  topics = log['topics'][1:]
  data = bytes()
  for t in topics:
    data += t
  data += log['data']
  decoded = client.codec.decode(types=topics_first, data=data)
  return tuple(reorder_decoded_params(list(decoded), indexed))


def reorder_decoded_params(decoded: list, indexed: list[bool]) -> list:
  """
  Reorders decoded parameters from [indexed_params..., non_indexed_params...]
  back to their original order based on the indexed flag list.

  Args:
    decoded: List with indexed params first, then non-indexed params
    indexed: Boolean list indicating which positions should be indexed

  Returns:
    List with parameters in their original order
  """
  reordered = []
  indexed_ptr = 0  # pointer for indexed params
  non_indexed_ptr = sum(
      indexed)  # pointer for non-indexed params (start after all indexed)

  for is_indexed in indexed:
    if is_indexed:
      reordered.append(decoded[indexed_ptr])
      indexed_ptr += 1
    else:
      reordered.append(decoded[non_indexed_ptr])
      non_indexed_ptr += 1

  return reordered


async def schedule(ing: Ingester) -> list[Task]:

  contracts: set[str] = set()
  data_by_event: dict[str, dict] = {}
  index_first_types_by_event: dict[str, list[str]] = {}
  event_hashes: dict[str, str] = {}
  events_by_contract: dict[str, set[str]] = {}
  filter_by_contract: dict[str, dict] = {}
  filter_index_by_event: dict[str, int] = {}
  last_block_by_contract: dict[str, int] = {}

  for field in ing.fields:
    contracts.add(field.target)
    events_by_contract.setdefault(field.target, set()).add(field.selector)

  for contract in contracts:

    chain_id, addr = split_chain_addr(contract)
    addr = Web3.to_checksum_address(addr)  # enforce checksum

    for event in events_by_contract[contract].copy(
    ):  # copy to avoid modifying while iterating
      event_name, param_types, indexed = parse_event_signature(event)
      event_id = f"{contract}:{event}"

      event_hash = Web3.keccak(text=event.replace('indexed ', '')).hex()
      event_hashes[event_id] = event_hash
      event_hashes[event_hash] = event_id

      # Separate indexed and non-indexed types
      index_types = [
          param_types[i] for i, is_indexed in enumerate(indexed) if is_indexed
      ]
      non_index_types = [
          param_types[i] for i, is_indexed in enumerate(indexed)
          if not is_indexed
      ]
      index_first_types_by_event[event_id] = index_types + non_index_types

      events_by_contract.setdefault(contract, set()).add(event_id)
      data_by_event.setdefault(event_id, {})

      # Build filter
      filter_by_contract.setdefault(
          contract, {
              "fromBlock": "latest",
              "toBlock": "latest",
              "address": addr,
              "topics": []
          })["topics"].append(event_hashes[event_id])

      filter_index_by_event[event_id] = len(
          filter_by_contract[contract]["topics"]) - 1

    filter_by_contract[contract]["topics"] = list(
        set(filter_by_contract[contract]["topics"]))  # remove duplicates

  async def poll_events(contract: str):
    chain_id, addr = split_chain_addr(contract)
    client = await state.web3.client(chain_id)
    f = filter_by_contract[contract]

    if not hasattr(client, 'eth'):
      log_error(f"Non-EVM client for chain {chain_id}, cannot poll events")
      return

    current_block = client.eth.block_number
    last_block_by_contract.setdefault(contract, current_block)
    prev_block = last_block_by_contract[contract]

    for retry_count in range(state.args.max_retries):
      if state.args.verbose:
        log_debug(f"Polling for {contract} events...")

      start_block = prev_block
      end_block = current_block
      f.update({"fromBlock": hex(start_block), "toBlock": hex(end_block)})

      if start_block >= end_block:
        log_info(
            f"No new blocks for {contract}, skipping event polling for {ing.interval}"
        )
        break

      try:
        logs = client.eth.get_logs(f)
        for log_entry in logs:
          event_id = event_hashes[log_entry["topics"][0].hex()]
          decoded_event = decode_log_data(client, log_entry,
                                          index_first_types_by_event[event_id],
                                          indexed)
          if state.args.verbose:
            log_debug(
                f"Block: {log_entry['blockNumber']} | Event: {decoded_event}")
        start_block = end_block + 1
        break
      except Exception as error:
        log_error(
            f"Failed to poll event logs for contract {contract}: {error}")
        # RPC rotation is handled automatically by Web3Proxy

  async def ingest(ing: Ingester):
    await ing.pre_ingest()

    tp = state.thread_pool
    future_by_contract = {
        contract: tp.submit(poll_events, contract)
        for contract in contracts
    }

    for field in ing.fields:
      try:
        field.value = future_by_contract[field.target].result(
            timeout=state.args.ingestion_timeout)
      except Exception as e:
        log_error(f"Failed to poll events for {field.target}: {e}")

    await ing.post_ingest()

  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
