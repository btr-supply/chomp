"""Tests for adapters.evm_rpc module."""
import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from chomp.src.utils.deps import safe_import

# Check if EVM dependencies are available
eth_utils = safe_import("eth_utils")
hexbytes = safe_import("hexbytes")
eth_account = safe_import("eth_account")
EVM_AVAILABLE = all(
    [eth_utils is not None, hexbytes is not None, eth_account is not None])

# Only import if dependencies are available
if EVM_AVAILABLE:
  from hexbytes import HexBytes
  from src.adapters.evm_rpc import EvmRpcClient


@pytest.mark.skipif(
    not EVM_AVAILABLE,
    reason="EVM dependencies not available (eth_utils, hexbytes, eth_account)")
class TestEvmRpcClient:
  """Test EVM RPC client functionality."""

  def test_inheritance(self):
    """Test EvmRpcClient inherits from JsonRpcClient."""
    from src.adapters.jsonrpc import JsonRpcClient
    assert issubclass(EvmRpcClient, JsonRpcClient)

  @pytest.mark.asyncio
  async def test_get_block_number(self):
    """Test getting block number."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0x123abc"

      result = await client.get_block_number()

      mock_call.assert_called_once_with("eth_blockNumber")
      assert result == 0x123abc

  @pytest.mark.asyncio
  async def test_get_gas_price(self):
    """Test getting gas price."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0x5d21dba00"  # 25 gwei in hex

      result = await client.get_gas_price()

      mock_call.assert_called_once_with("eth_gasPrice")
      assert result == 25000000000

  @pytest.mark.asyncio
  async def test_get_priority_fees(self):
    """Test getting priority fees."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_fees = {"maxPriorityFeePerGas": "0x3b9aca00"}
      mock_call.return_value = expected_fees

      result = await client.get_priority_fees()

      mock_call.assert_called_once_with("eth_maxPriorityFeePerGas")
      assert result == expected_fees

  @pytest.mark.asyncio
  async def test_get_balance(self):
    """Test getting account balance."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0xde0b6b3a7640000"  # 1 ETH in wei

      result = await client.get_balance(address)

      mock_call.assert_called_once_with("eth_getBalance", [address, "latest"])
      assert result == 1000000000000000000

  @pytest.mark.asyncio
  async def test_call_contract_method_no_params(self):
    """Test calling contract method without parameters."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"
    method = "totalSupply()"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0x1234"

      result = await client.call_contract_method(address, method)

      # Verify call was made with proper method signature
      args = mock_call.call_args[0]
      assert args[0] == "eth_call"
      payload = args[1][0]
      assert payload["to"] == address
      assert payload["data"].startswith("0x")
      assert isinstance(result, HexBytes)

  @pytest.mark.asyncio
  async def test_call_contract_method_with_params(self):
    """Test calling contract method with parameters."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"
    method = "balanceOf(address)"
    params = ["0xabcdef1234567890123456789012345678901234"]

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0x5678"

      result = await client.call_contract_method(address, method, params)

      mock_call.assert_called_once()
      assert isinstance(result, HexBytes)

  def test_encode_param_integer(self):
    """Test encoding integer parameters."""
    result = EvmRpcClient._encode_param(123)
    assert result == f"{123:064x}"
    assert len(result) == 64

  def test_encode_param_hex_string(self):
    """Test encoding hex string parameters."""
    hex_value = "0x1234"
    result = EvmRpcClient._encode_param(hex_value)
    assert result == "1234".zfill(64)
    assert len(result) == 64

  def test_encode_param_regular_string(self):
    """Test encoding regular string parameters."""
    text = "hello"
    result = EvmRpcClient._encode_param(text)
    expected = text.encode("utf-8").hex().zfill(64)
    assert result == expected
    assert len(result) == 64

  def test_encode_param_unsupported_type(self):
    """Test encoding unsupported parameter types raises error."""
    with pytest.raises(ValueError, match="Unsupported parameter type"):
      EvmRpcClient._encode_param([1, 2, 3])

  @pytest.mark.asyncio
  async def test_get_storage_at(self):
    """Test getting storage at specific slot."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"
    slot = 5

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0xabcdef"

      result = await client.get_storage_at(address, slot)

      mock_call.assert_called_once_with("eth_getStorageAt",
                                        [address, hex(slot), "latest"])
      assert result == "0xabcdef"

  @pytest.mark.asyncio
  async def test_get_block_by_number(self):
    """Test getting block by number."""
    client = EvmRpcClient("http://localhost:8545")
    block_number = 12345

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_block = {"number": "0x3039", "hash": "0xabc123"}
      mock_call.return_value = expected_block

      result = await client.get_block(block_number, full_transactions=True)

      mock_call.assert_called_once_with("eth_getBlockByNumber",
                                        [hex(block_number), True])
      assert result == expected_block

  @pytest.mark.asyncio
  async def test_get_block_by_string(self):
    """Test getting block by string identifier."""
    client = EvmRpcClient("http://localhost:8545")
    block_id = "latest"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_block = {"number": "0x3039", "hash": "0xabc123"}
      mock_call.return_value = expected_block

      result = await client.get_block(block_id, full_transactions=False)

      mock_call.assert_called_once_with("eth_getBlockByNumber",
                                        [block_id, False])
      assert result == expected_block

  @pytest.mark.asyncio
  async def test_get_transaction(self):
    """Test getting transaction by hash."""
    client = EvmRpcClient("http://localhost:8545")
    tx_hash = "0xabcdef1234567890"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_tx = {"hash": tx_hash, "from": "0x123", "to": "0x456"}
      mock_call.return_value = expected_tx

      result = await client.get_transaction(tx_hash)

      mock_call.assert_called_once_with("eth_getTransactionByHash", [tx_hash])
      assert result == expected_tx

  @pytest.mark.asyncio
  async def test_get_transaction_receipt(self):
    """Test getting transaction receipt."""
    client = EvmRpcClient("http://localhost:8545")
    tx_hash = "0xabcdef1234567890"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_receipt = {"transactionHash": tx_hash, "status": "0x1"}
      mock_call.return_value = expected_receipt

      result = await client.get_transaction_receipt(tx_hash)

      mock_call.assert_called_once_with("eth_getTransactionReceipt", [tx_hash])
      assert result == expected_receipt

  @pytest.mark.asyncio
  async def test_get_code(self):
    """Test getting contract code."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_code = "0x608060405234801561001057600080fd5b50"
      mock_call.return_value = expected_code

      result = await client.get_code(address)

      mock_call.assert_called_once_with("eth_getCode", [address, "latest"])
      assert result == expected_code

  @pytest.mark.asyncio
  async def test_get_code_with_block(self):
    """Test getting contract code at specific block."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"
    block = "0x123"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0x6080"

      await client.get_code(address, block)

      mock_call.assert_called_once_with("eth_getCode", [address, block])

  @pytest.mark.asyncio
  async def test_get_logs_minimal(self):
    """Test getting logs with minimal parameters."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_logs = [{"address": "0x123", "data": "0xabc"}]
      mock_call.return_value = expected_logs

      result = await client.get_logs()

      expected_params = {"fromBlock": "latest", "toBlock": "latest"}
      mock_call.assert_called_once_with("eth_getLogs", [expected_params])
      assert result == expected_logs

  @pytest.mark.asyncio
  async def test_get_logs_with_parameters(self):
    """Test getting logs with all parameters."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"
    topics = [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    ]

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = []

      await client.get_logs(from_block=1000,
                            to_block=2000,
                            address=address,
                            topics=topics)

      expected_params = {
          "fromBlock": hex(1000),
          "toBlock": hex(2000),
          "address": address,
          "topics": topics
      }
      mock_call.assert_called_once_with("eth_getLogs", [expected_params])

  @pytest.mark.asyncio
  async def test_get_transaction_count(self):
    """Test getting transaction count (nonce)."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0xa"  # 10 in hex

      result = await client.get_transaction_count(address)

      mock_call.assert_called_once_with("eth_getTransactionCount",
                                        [address, "latest"])
      assert result == 10

  @pytest.mark.asyncio
  async def test_estimate_gas(self):
    """Test estimating gas for transaction."""
    client = EvmRpcClient("http://localhost:8545")
    transaction = {"to": "0x123", "value": "0x1234"}

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0x5208"  # 21000 in hex

      result = await client.estimate_gas(transaction)

      mock_call.assert_called_once_with("eth_estimateGas", [transaction])
      assert result == 21000

  @pytest.mark.asyncio
  async def test_send_raw_transaction(self):
    """Test sending raw signed transaction."""
    client = EvmRpcClient("http://localhost:8545")
    signed_tx = "0xf86c0a8509184e72a00082271094000000000000000000000000000000000000000080a47f7465737432000000000000000000000000000000000000000000000000000000006000571ca08a8bbf888cfa37bbf0bb965423625641fc956967b81d12e23709cead01446075a0a2bf8b9b5e4d3ad7ce8e3e2b6f39d7c7e3d9f8e7d3e1e9d7e9e1e6e2e1e4e5e6"
    expected_hash = "0xabcdef1234567890"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = expected_hash

      result = await client.send_raw_transaction(signed_tx)

      mock_call.assert_called_once_with("eth_sendRawTransaction", [signed_tx])
      assert result == expected_hash

  @pytest.mark.asyncio
  async def test_get_chain_id(self):
    """Test getting chain ID."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = "0x1"  # Ethereum mainnet

      result = await client.get_chain_id()

      mock_call.assert_called_once_with("eth_chainId")
      assert result == 1

  @pytest.mark.asyncio
  async def test_get_block_by_hash(self):
    """Test getting block by hash."""
    client = EvmRpcClient("http://localhost:8545")
    block_hash = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_block = {"hash": block_hash, "number": "0x123"}
      mock_call.return_value = expected_block

      result = await client.get_block_by_hash(block_hash, True)

      mock_call.assert_called_once_with("eth_getBlockByHash",
                                        [block_hash, True])
      assert result == expected_block

  @pytest.mark.asyncio
  async def test_create_filter(self):
    """Test creating event filter."""
    client = EvmRpcClient("http://localhost:8545")
    address = "0x1234567890123456789012345678901234567890"
    topics = [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    ]

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      filter_id = "0x1"
      mock_call.return_value = filter_id

      result = await client.create_filter(from_block=1000,
                                          to_block=2000,
                                          address=address,
                                          topics=topics)

      expected_params = {
          "fromBlock": hex(1000),
          "toBlock": hex(2000),
          "address": address,
          "topics": topics
      }
      mock_call.assert_called_once_with("eth_newFilter", [expected_params])
      assert result == filter_id

  @pytest.mark.asyncio
  async def test_create_block_filter(self):
    """Test creating block filter."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      filter_id = "0x2"
      mock_call.return_value = filter_id

      result = await client.create_block_filter()

      mock_call.assert_called_once_with("eth_newBlockFilter")
      assert result == filter_id

  @pytest.mark.asyncio
  async def test_get_filter_changes(self):
    """Test getting filter changes."""
    client = EvmRpcClient("http://localhost:8545")
    filter_id = "0x1"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      expected_changes = [{"address": "0x123", "data": "0xabc"}]
      mock_call.return_value = expected_changes

      result = await client.get_filter_changes(filter_id)

      mock_call.assert_called_once_with("eth_getFilterChanges", [filter_id])
      assert result == expected_changes

  @pytest.mark.asyncio
  async def test_uninstall_filter(self):
    """Test uninstalling filter."""
    client = EvmRpcClient("http://localhost:8545")
    filter_id = "0x1"

    with patch.object(client, 'call', new_callable=AsyncMock) as mock_call:
      mock_call.return_value = True

      result = await client.uninstall_filter(filter_id)

      mock_call.assert_called_once_with("eth_uninstallFilter", [filter_id])
      assert result is True

  @pytest.mark.asyncio
  async def test_verify_message_success(self):
    """Test successful message verification."""
    client = EvmRpcClient("http://localhost:8545")

    with patch('src.adapters.evm_rpc.encode_defunct') as mock_encode, \
         patch('src.adapters.evm_rpc.Account.recover_message') as mock_recover:

      message = "Hello, World!"
      signature = "0x1234567890abcdef"
      expected_address = "0xabcdef1234567890123456789012345678901234"

      mock_recover.return_value = expected_address

      result = await client.verify_message(message, signature,
                                           expected_address)

      mock_encode.assert_called_once_with(text=message)
      mock_recover.assert_called_once()
      assert result is True

  @pytest.mark.asyncio
  async def test_verify_message_failure(self):
    """Test message verification failure."""
    client = EvmRpcClient("http://localhost:8545")

    with patch('src.adapters.evm_rpc.Account.recover_message',
               side_effect=Exception("Invalid signature")):

      result = await client.verify_message("message", "signature", "address")

      assert result is False

  def test_verify_signature_success(self):
    """Test successful signature verification."""
    client = EvmRpcClient("http://localhost:8545")

    with patch('src.adapters.evm_rpc.Account.recover_message') as mock_recover:
      message_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
      signature = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1b"
      expected_address = "0xabcdef1234567890123456789012345678901234"

      mock_recover.return_value = expected_address

      result = client.verify_signature(message_hash, signature,
                                       expected_address)

      mock_recover.assert_called_once()
      assert result is True

  def test_verify_signature_invalid_format(self):
    """Test signature verification with invalid signature format."""
    client = EvmRpcClient("http://localhost:8545")

    # Test with signature that's too short to parse
    result = client.verify_signature("0x1234", "0x123", "0xaddress")
    assert result is False

  def test_verify_signature_failure(self):
    """Test signature verification failure."""
    client = EvmRpcClient("http://localhost:8545")

    with patch('src.adapters.evm_rpc.Account.recover_message',
               side_effect=Exception("Invalid")):

      result = client.verify_signature("hash", "signature", "address")

      assert result is False

  @pytest.mark.asyncio
  async def test_verify_transaction_success(self):
    """Test successful transaction verification."""
    client = EvmRpcClient("http://localhost:8545")
    tx_hash = "0xabcdef1234567890"
    expected_address = "0x1234567890123456789012345678901234567890"

    with patch.object(client, 'get_transaction', new_callable=AsyncMock) as mock_get_tx, \
         patch.object(client, 'get_transaction_receipt', new_callable=AsyncMock) as mock_get_receipt:

      mock_get_tx.return_value = {"from": expected_address, "to": "0x456"}
      mock_get_receipt.return_value = {"status": "0x1"}

      result = await client.verify_transaction(tx_hash, expected_address)

      mock_get_tx.assert_called_once_with(tx_hash)
      mock_get_receipt.assert_called_once_with(tx_hash)
      assert result is True

  @pytest.mark.asyncio
  async def test_verify_transaction_no_tx(self):
    """Test transaction verification when transaction not found."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'get_transaction',
                      new_callable=AsyncMock) as mock_get_tx:
      mock_get_tx.return_value = None

      result = await client.verify_transaction("tx_hash", "address")

      assert result is False

  @pytest.mark.asyncio
  async def test_verify_transaction_no_receipt(self):
    """Test transaction verification when receipt not found."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'get_transaction', new_callable=AsyncMock) as mock_get_tx, \
         patch.object(client, 'get_transaction_receipt', new_callable=AsyncMock) as mock_get_receipt:

      mock_get_tx.return_value = {"from": "0x123"}
      mock_get_receipt.return_value = None

      result = await client.verify_transaction("tx_hash", "address")

      assert result is False

  @pytest.mark.asyncio
  async def test_verify_transaction_wrong_sender(self):
    """Test transaction verification with wrong sender."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client, 'get_transaction', new_callable=AsyncMock) as mock_get_tx, \
         patch.object(client, 'get_transaction_receipt', new_callable=AsyncMock) as mock_get_receipt:

      mock_get_tx.return_value = {"from": "0x111"}
      mock_get_receipt.return_value = {"status": "0x1"}

      result = await client.verify_transaction("tx_hash", "0x222")

      assert result is False

  @pytest.mark.asyncio
  async def test_verify_transaction_failed_status(self):
    """Test transaction verification with failed transaction."""
    client = EvmRpcClient("http://localhost:8545")
    expected_address = "0x1234567890123456789012345678901234567890"

    with patch.object(client, 'get_transaction', new_callable=AsyncMock) as mock_get_tx, \
         patch.object(client, 'get_transaction_receipt', new_callable=AsyncMock) as mock_get_receipt:

      mock_get_tx.return_value = {"from": expected_address}
      mock_get_receipt.return_value = {"status": "0x0"}  # Failed transaction

      result = await client.verify_transaction("tx_hash", expected_address)

      assert result is False

  @pytest.mark.asyncio
  async def test_verify_transaction_exception(self):
    """Test transaction verification with exception."""
    client = EvmRpcClient("http://localhost:8545")

    with patch.object(client,
                      'get_transaction',
                      new_callable=AsyncMock,
                      side_effect=Exception("Network error")):

      result = await client.verify_transaction("tx_hash", "address")

      assert result is False
