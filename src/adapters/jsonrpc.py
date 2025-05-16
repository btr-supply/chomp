import orjson
import httpx
from typing import Optional

class JsonRpcClient:
  def __init__(self, endpoint: str, headers: Optional[dict[str, str]] = None, timeout: float = 10.0, jsonrpc_version: str = "2.0"):
    self.endpoint = endpoint
    self.headers = headers or {"Content-Type": "application/json"}
    self.timeout = timeout
    self.jsonrpc_version = jsonrpc_version
    self._client: Optional[httpx.AsyncClient] = None
    self.connect()

  def connect(self) -> None:
    if not self._client:
      self._client = httpx.AsyncClient(timeout=self.timeout)

  async def disconnect(self) -> None:
    if self._client:
      await self._client.aclose()
      self._client = None

  async def reconnect(self) -> None:
    await self.disconnect()
    await self.connect()

  async def ping(self) -> bool:
    try:
      await self.call("getHealth")
      return True
    except:
      return False

  async def is_connected(self) -> bool:
    if not self._client:
      return False
    return await self.ping()

  async def call(self, method: str, params: Optional[any] = None, request_id: int = 1, ensure_connected=True) -> any:
    if ensure_connected and not await self.is_connected():
      self.connect()
      if not await self.is_connected():
        raise Exception("Failed to establish connection")

    payload = {
      "jsonrpc": self.jsonrpc_version,
      "method": method,
      "params": params or [],
      "id": request_id,
    }

    # Serialize payload to JSON and calculate Content-Length
    json_payload = orjson.dumps(payload, option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_SERIALIZE_DATACLASS)
    headers = self.headers.copy()
    headers["Content-Length"] = str(len(json_payload))
    try:
      response = await self._client.post(self.endpoint, data=json_payload, headers=headers)
      response.raise_for_status()  # Raise an error for HTTP-level issues
      data = response.json()
    except Exception as e:
      raise Exception(f"Failed to connect to {self.endpoint}: {e}")

    if "error" in data:
      raise Exception(f"JSON-RPC Error: {data['error']}")

    return data.get("result")
