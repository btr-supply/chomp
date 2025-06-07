from typing import Optional, Union, Any
from eth_utils import keccak
from hexbytes import HexBytes
from eth_account.messages import encode_defunct
from eth_account import Account

from .jsonrpc import JsonRpcClient


class EvmRpcClient(JsonRpcClient):

  async def get_block_number(self) -> int:
    return int(await self.call("eth_blockNumber"), 16)

  async def get_gas_price(self) -> int:
    return int(await self.call("eth_gasPrice"), 16)

  async def get_priority_fees(self) -> dict:
    return await self.call("eth_maxPriorityFeePerGas")  # EIP-1559

  async def get_balance(self, address: str) -> int:
    return int(await self.call("eth_getBalance", [address, "latest"]), 16)

  async def call_contract_method(self,
                                 address: str,
                                 method: str,
                                 params: list[Any] = []) -> Any:
    method_id = keccak(text=method)[:4]  # First 4 bytes of keccak hash
    encoded_params = "".join([self._encode_param(param) for param in params])
    data = f"0x{method_id.hex()}{encoded_params}"

    payload = {
        "to": address,
        "data": data,
    }
    result = await self.call("eth_call", [payload, "latest"])
    return HexBytes(result)

  async def get_storage_at(self, address: str, slot: int) -> str:
    return await self.call("eth_getStorageAt", [address, hex(slot), "latest"])

  @staticmethod
  def _encode_param(param: Any) -> str:
    """Encode a single parameter for an EVM contract call."""
    if isinstance(param, int):
      return f"{param:064x}"  # Convert integer to padded hex
    elif isinstance(param, str):
      if param.startswith("0x"):
        return param[2:].zfill(64)
      return param.encode("utf-8").hex().zfill(64)
    raise ValueError(f"Unsupported parameter type: {type(param)}")

  async def get_block(self,
                      block_id: Union[int, str],
                      full_transactions: bool = False) -> dict:
    return await self.call("eth_getBlockByNumber", [
        hex(block_id) if isinstance(block_id, int) else block_id,
        full_transactions
    ])

  async def get_transaction(self, tx_hash: str) -> Optional[dict]:
    return await self.call("eth_getTransactionByHash", [tx_hash])

  async def get_transaction_receipt(self, tx_hash: str) -> Optional[dict]:
    return await self.call("eth_getTransactionReceipt", [tx_hash])

  async def get_code(self, address: str, block: str = "latest") -> str:
    return await self.call("eth_getCode", [address, block])

  async def get_logs(self,
                     from_block: Union[int, str] = "latest",
                     to_block: Union[int, str] = "latest",
                     address: Optional[Union[str, list[str]]] = None,
                     topics: Optional[list[Any]] = None) -> list[dict]:
    params = {
        "fromBlock":
        hex(from_block) if isinstance(from_block, int) else from_block,
        "toBlock": hex(to_block) if isinstance(to_block, int) else to_block
    }
    if address is not None:
      params["address"] = address  # type: ignore
    if topics is not None:
      params["topics"] = topics  # type: ignore
    return await self.call("eth_getLogs", [params])

  async def get_transaction_count(self,
                                  address: str,
                                  block: str = "latest") -> int:
    return int(await self.call("eth_getTransactionCount", [address, block]),
               16)

  async def estimate_gas(self, transaction: dict) -> int:
    return int(await self.call("eth_estimateGas", [transaction]), 16)

  async def send_raw_transaction(self, signed_tx: str) -> str:
    return await self.call("eth_sendRawTransaction", [signed_tx])

  async def get_chain_id(self) -> int:
    return int(await self.call("eth_chainId"), 16)

  async def get_block_by_hash(self,
                              block_hash: str,
                              full_transactions: bool = False) -> dict:
    return await self.call("eth_getBlockByHash",
                           [block_hash, full_transactions])

  async def get_filter_changes(self, filter_id: str) -> list[dict]:
    return await self.call("eth_getFilterChanges", [filter_id])

  async def create_filter(self,
                          from_block: Union[int, str] = "latest",
                          to_block: Union[int, str] = "latest",
                          address: Optional[Union[str, list[str]]] = None,
                          topics: Optional[list[Any]] = None) -> str:
    params = {
        "fromBlock":
        hex(from_block) if isinstance(from_block, int) else from_block,
        "toBlock": hex(to_block) if isinstance(to_block, int) else to_block
    }
    if address is not None:
      params["address"] = address  # type: ignore
    if topics is not None:
      params["topics"] = topics  # type: ignore
    return await self.call("eth_newFilter", [params])

  async def create_block_filter(self) -> str:
    return await self.call("eth_newBlockFilter")

  async def uninstall_filter(self, filter_id: str) -> bool:
    return await self.call("eth_uninstallFilter", [filter_id])

  async def verify_message(self, message: str, signature: str,
                           expected_address: str) -> bool:
    """Verify an Ethereum signed message"""
    try:
      message_hash = encode_defunct(text=message)
      recovered_address = Account.recover_message(message_hash,
                                                  signature=signature)
      return recovered_address.lower() == expected_address.lower()
    except Exception:
      return False

  def verify_signature(self, message_hash: str, signature: str,
                       expected_address: str) -> bool:
    """Verify a raw signature against a message hash"""
    try:
      # Remove '0x' prefix if present
      message_hash = message_hash.replace('0x', '')
      signature = signature.replace('0x', '')

      # Recover the public key and address
      r = int(signature[:64], 16)
      s = int(signature[64:128], 16)
      v = int(signature[128:], 16)

      recovered_address = Account.recover_message(message_hash, vrs=(v, r, s))
      return recovered_address.lower() == expected_address.lower()
    except Exception:
      return False

  async def verify_transaction(self, tx_hash: str,
                               expected_address: str) -> bool:
    """Verify a transaction was sent by an address and was successful"""
    try:
      tx = await self.get_transaction(tx_hash)
      if not tx:
        return False

      receipt = await self.get_transaction_receipt(tx_hash)
      if not receipt:
        return False

      # Check sender address and transaction status
      return (tx.get("from", "").lower() == expected_address.lower()
              and receipt.get("status") == "0x1"  # 0x1 means success
              )
    except Exception:
      return False
