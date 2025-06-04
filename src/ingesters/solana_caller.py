from asyncio import Task
import base64
from typing import Any, Dict
from ..model import Ingester, ResourceField
from ..utils import log_debug, log_error
from ..actions import transform_and_store, scheduler
from ..cache import ensure_claim_task
from .. import state
from ..adapters.solana_rpc import SolanaRpcClient

max_batch_size = 100 # Solana has a limit of 100 accounts per getMultipleAccounts request

def parse_generic(data: Any) -> Any:
  return data

async def schedule(c: Ingester) -> list[Task]:
  async def ingest(c: Ingester):
    await ensure_claim_task(c)
    data_by_account = {}
    b64_data_by_account = {}
    account_cache = {}

    async def fetch_accounts_batch(fields: list[ResourceField], max_retries: int = state.args.max_retries, max_batch_size=100) -> Dict[str, Any]:
      retry_count = 0
      results: Dict[str, Any] = {}

      # Get unique accounts while preserving order
      unique_accounts = list({f.target for f in fields if f.target})

      # Split accounts into batches
      for i in range(0, len(unique_accounts), max_batch_size):
        batch = unique_accounts[i:i + max_batch_size]

        while retry_count < max_retries:
          client = await state.web3.client("solana", roll=True)
          try:
            # Type guard: ensure we have a SolanaRpcClient
            if not isinstance(client, SolanaRpcClient):
              raise TypeError(f"Expected SolanaRpcClient, got {type(client)}")

            if state.args.verbose:
              log_debug(f"Fetching batch {i//max_batch_size + 1} of {(len(unique_accounts) + max_batch_size - 1)//max_batch_size}")

            # Create a mapping of index to account address for this batch
            batch_index_map = {i: addr for i, addr in enumerate(batch)}

            # Fetch accounts for this batch
            accounts_info = await client.get_multi_accounts(batch, encoding="base64")

            # Process results using the index mapping
            for i, account in enumerate(accounts_info):
              if account is None:
                continue

              account_address = batch_index_map[i]
              account_data = account.get("data", [None, None])[0]

              b64_data_by_account[account_address] = account_data
              if account_data:
                data_by_account[account_address] = base64.b64decode(account_data)

            break # Success, move to next batch

          except Exception as e:
            log_error(f"Error fetching accounts batch {i//max_batch_size + 1}, switching RPC... ({str(e)})")
            # Type guard for endpoint access
            if isinstance(client, SolanaRpcClient):
              prev_rpc = client.endpoint
            else:
              prev_rpc = "unknown"
            client = await state.web3.client("solana", roll=True)
            if isinstance(client, SolanaRpcClient):
              new_rpc = client.endpoint
            else:
              new_rpc = "unknown"
            if state.args.verbose:
              log_debug(f"Switched RPC {prev_rpc} -> {new_rpc}")
            retry_count += 1

        if retry_count >= max_retries:
          log_error(f"Failed to fetch accounts batch after {max_retries} retries")
          return results

      # Process results for all fields after all batches are fetched
      for field in fields:
        if field.target not in data_by_account:
          log_error(f"Account not found: {field.target}")
          continue

        if field.selector and data_by_account[field.target]:
          data = data_by_account[field.target]
          if field.selector:
            selector_parts = field.selector.split(',')
            selected_data: list[bytes] = []
            for selector in selector_parts:
              start, end = map(int, selector.split(':'))
              selected_data.append(data[start:end])
            # Fix: assign the list directly instead of individual bytes
            results[field.target] = selected_data
          else:
            results[field.target] = data
          account_cache[field.target] = results[field.target]

      return results

    # Fetch all accounts
    batch_results = await fetch_accounts_batch(c.fields)

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
