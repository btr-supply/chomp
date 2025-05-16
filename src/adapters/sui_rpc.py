from .jsonrpc import JsonRpcClient
from typing import Optional, Union

class SuiRpcClient(JsonRpcClient):

  # default query parameters
  default_filters = {
    "showType": True,
    "showOwner": False,
    "showPreviousTransaction": False,
    "showDisplay": False,
    "showContent": True,
    "showBcs": False,
    "showStorageRebate": False
  }

  def construct_query_params(
    self,
    **kwargs: bool
  ) -> list[Union[str, list[str], dict[str, bool]]]:
    return [
      {key: value for key, value in {**self.default_filters, **kwargs}.items()}
    ]

  async def get_protocol_config(self) -> str:
    return await self.call("sui_getProtocolConfig", ensure_connected=False)

  async def get_version(self) -> str:
    data = await self.get_protocol_config()
    return data.get("protocolVersion")

  async def get_chain_id(self) -> str:
    return await self.call("sui_getChainIdentifier", ensure_connected=False)

  async def is_connected(self) -> str:
    chain_id = await self.get_chain_id() # hex string
    return chain_id is not None

  async def get_object(
    self,
    object_id: str,
    **kwargs: bool
  ) -> Optional[dict]:
    params = self.construct_query_params(**kwargs)
    params.insert(0, object_id)
    result = await self.call("sui_getObject", params)
    return result.get("data") if result else None

  async def get_multi_objects(
    self,
    object_ids: list[str],
    **kwargs: bool
  ) -> list[Optional[dict]]:
    params = self.construct_query_params(**kwargs)
    params.insert(0, object_ids)
    results = await self.call("sui_multiGetObjects", params)
    return [result.get("data") for result in results] if results else []

  async def get_object_fields(self, object_id: str) -> dict:
    o = await self.get_object(object_id)
    return o.get("content", {}).get("fields", {})

  async def get_multi_object_fields(self, object_ids: list[str]) -> list[dict]:
    objects = await self.get_multi_objects(object_ids)
    return [obj.get("content", {}).get("fields", {}) for obj in objects]
