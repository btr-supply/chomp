from asyncio import Task
import base64
from typing import Any

from ..models.ingesters import Ingester
from ..models.base import ResourceField
from ..utils import log_error, log_debug
from ..actions import scheduler
from ..adapters.svm_rpc import SvmRpcClient
from .. import state


async def schedule(ing: Ingester) -> list[Task]:

  async def ingest(ing: Ingester):
    await ing.pre_ingest()

    data_by_account = {}
    b64_data_by_account = {}

    async def fetch_accounts_batch(
        fields: list[ResourceField]) -> dict[str, Any]:
      results: dict[str, Any] = {}
      unique_accounts = list({f.target for f in fields if f.target})
      max_batch_size = 100  # Solana limit

      # Process accounts in batches
      for i in range(0, len(unique_accounts), max_batch_size):
        batch = unique_accounts[i:i + max_batch_size]

        try:
          client = await state.web3.client("solana", roll=True)
          if not isinstance(client, SvmRpcClient):
            raise TypeError(f"Expected SvmRpcClient, got {type(client)}")

          if state.args.verbose:
            batch_num = i // max_batch_size + 1
            total_batches = (len(unique_accounts) + max_batch_size -
                             1) // max_batch_size
            log_debug(
                f"Fetching batch {batch_num}/{total_batches} from RPC: {client.endpoint}"
            )

          # Fetch accounts for this batch
          accounts_info = await client.get_multi_accounts(batch,
                                                          encoding="base64")

          # Process results
          for idx, account in enumerate(accounts_info):
            if account is None:
              continue

            account_address = batch[idx]
            account_data = account.get("data", [None, None])[0]

            b64_data_by_account[account_address] = account_data
            if account_data:
              data_by_account[account_address] = base64.b64decode(account_data)

        except Exception as e:
          log_error(
              f"Error fetching accounts batch {i // max_batch_size + 1}: {e}")
          continue

      # Process results for all fields
      for field in fields:
        if field.target not in data_by_account:
          field_names_for_account = [
              f.name for f in fields if f.target == field.target
          ]
          all_configured_accounts = set(f.target for f in ing.fields
                                        if f.target)

          ing.log_resource_not_found(
              resource_type="Account",
              resource_id=field.target,
              field_names=field_names_for_account,
              endpoint=getattr(client, 'endpoint', 'unknown'),
              retries_exhausted=False,
              batch_size=len(unique_accounts),
              total_fields=len(ing.fields),
              total_unique_resources=len(all_configured_accounts))
          continue

        data = data_by_account[field.target]
        if not field.selector:
          results[field.target] = data
          continue

        # Multi selector parsing: support comma-separated selectors
        selector_parts = field.selector.split(",")
        selected_data: list[bytes] = []
        for selector in selector_parts:
          try:
            start, end = map(int, selector.split(":"))
            selected_data.append(data[start:end])
          except (ValueError, IndexError) as e:
            log_error(
                f"Error processing selector '{selector}' for field {field.name}: {e}"
            )
            continue
        results[field.target] = selected_data

      return results

    # Fetch all accounts and process results
    batch_results = await fetch_accounts_batch(ing.fields)
    ing.process_batch_results(batch_results)
    await ing.post_ingest(response_data=batch_results)

  task = await scheduler.add_ingester(ing, fn=ingest, start=False)
  return [task] if task is not None else []
