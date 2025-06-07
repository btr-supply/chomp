from .jsonrpc import JsonRpcClient
from typing import Optional


class SvmRpcClient(JsonRpcClient):

  async def get_health(self) -> str:
    return await self.call("getHealth", ensure_connected=False)

  async def is_connected(self) -> bool:
    return await self.get_health() == "ok"

  async def get_slot(self) -> int:
    return await self.call("getSlot", {"commitment": "finalized"})

  async def get_block(self, slot: int) -> dict:
    return await self.call("getBlock", [slot, {"encoding": "json"}])

  async def get_balance(self, address: str) -> int:
    return await self.call("getBalance",
                           [address, {
                               "commitment": "finalized"
                           }])

  async def get_account_info(self,
                             address: str,
                             encoding: str = "base64") -> Optional[dict]:
    result = await self.call("getAccountInfo",
                             [address, {
                                 "encoding": encoding
                             }])
    return result.get("value") if result else None

  async def get_token_balances(self, address: str) -> list[dict]:
    return await self.call(
        "getTokenAccountsByOwner",
        [
            address, {
                "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            }
        ],
    )

  async def get_signature_status(self, signature: str) -> Optional[dict]:
    result = await self.call("getSignatureStatuses",
                             [[signature], {
                                 "searchTransactionHistory": True
                             }])
    return result.get("value", [None])[0]

  async def get_token_supply(self, mint: str) -> dict:
    return await self.call("getTokenSupply", [mint])

  async def get_recent_blockhash(self) -> str:
    return (await self.call("getRecentBlockhash",
                            {"commitment": "finalized"}))["blockhash"]

  async def get_transaction(self,
                            signature: str,
                            encoding: str = "json") -> Optional[dict]:
    return await self.call(
        "getTransaction",
        [
            signature, {
                "encoding": encoding,
                "maxSupportedTransactionVersion": 0
            }
        ],
    )

  async def get_latest_blockhash(self) -> dict:
    return await self.call("getLatestBlockhash", {"commitment": "finalized"})

  async def get_multi_accounts(
      self,
      addresses: list[str],
      encoding: str = "base64") -> list[Optional[dict]]:
    result = await self.call("getMultipleAccounts",
                             [addresses, {
                                 "encoding": encoding
                             }])
    return result.get("value", []) if result else []

  async def get_program_accounts(
      self,
      program_id: str,
      encoding: str = "base64",
      filters: Optional[list[dict]] = None,
  ) -> list[dict]:
    config = {"encoding": encoding, "filters": filters or []}
    return await self.call("getProgramAccounts", [program_id, config])

  async def verify_signature(self, signature: str) -> bool:
    try:
      status = await self.get_signature_status(signature)
      if not status:
        return False
      # Check if transaction was confirmed and didn't fail
      return status.get(
          "confirmationStatus") == "finalized" and not status.get("err")
    except Exception:
      return False

  async def verify_signatures(self, signatures: list[str]) -> list[bool]:
    result = await self.call("getSignatureStatuses",
                             [signatures, {
                                 "searchTransactionHistory": True
                             }])
    statuses = result.get("value", []) if result else []
    return [
        bool(status and status.get("confirmationStatus") == "finalized"
             and not status.get("err")) for status in statuses
    ]
