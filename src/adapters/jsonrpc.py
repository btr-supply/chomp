import orjson
from typing import Any, Optional

from ..utils.http import post


class JsonRpcClient:

  def __init__(self,
               endpoint: str,
               headers: Optional[dict[str, str]] = None,
               timeout: float = 10.0,
               jsonrpc_version: str = "2.0"):
    self.endpoint = endpoint
    self.headers = headers or {"Content-Type": "application/json"}
    self.timeout = timeout
    self.jsonrpc_version = jsonrpc_version
    self.user: Optional[str] = None
    self.password: Optional[str] = None

  def set_auth(self, user: str, password: str) -> None:
    """Set authentication credentials."""
    self.user = user
    self.password = password

  def connect(self) -> None:
    # Connection is handled automatically by singleton HTTP client
    pass

  async def disconnect(self) -> None:
    # Singleton client is managed globally
    pass

  async def reconnect(self) -> None:
    # No need to reconnect with singleton
    pass

  async def ping(self) -> bool:
    try:
      await self.call("getHealth")
      return True
    except Exception:
      return False

  async def is_connected(self) -> bool:
    return await self.ping()

  async def call(self,
                 method: str,
                 params: Optional[Any] = None,
                 request_id: int = 1,
                 ensure_connected=True) -> Any:
    payload = {
        "jsonrpc": self.jsonrpc_version,
        "method": method,
        "params": params or [],
        "id": request_id,
    }

    # Serialize payload to JSON and calculate Content-Length
    json_payload = orjson.dumps(payload,
                                option=orjson.OPT_SERIALIZE_NUMPY
                                | orjson.OPT_SERIALIZE_DATACLASS)
    headers = self.headers.copy()
    headers["Content-Length"] = str(len(json_payload))

    try:
      response = await post(self.endpoint,
                            content=json_payload,
                            headers=headers,
                            user=self.user,
                            password=self.password)
      response.raise_for_status()  # Raise an error for HTTP-level issues
      data = response.json()
    except Exception as e:
      raise Exception(f"Failed to connect to {self.endpoint}: {e}")

    if "error" in data:
      raise Exception(f"JSON-RPC Error: {data['error']}")

    return data.get("result")
