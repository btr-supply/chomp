from hashlib import md5
from fastapi import Request


def requester_id(req: Request) -> str:
  """Get requester ID from request"""
  if hasattr(req, 'state') and hasattr(req.state, 'requester_id'):
    return req.state.requester_id
  if req.client is not None:
    return req.client.host
  return "unknown"


# TODO: implement pattern/geo tracking to flag rotating proxies
_id_cache: dict[str, str] = {}


def hashed_requester_id(req: Request, id_salt: str = "reqid:") -> str:
  """Get hashed requester ID from request IP"""
  ip = req.client.host if req.client is not None else "unknown"
  if ip in _id_cache:
    return _id_cache[ip]
  salted = f"{id_salt}{ip}".encode('utf-8')
  hashed = md5(salted).hexdigest()
  _id_cache[ip] = hashed  # cache the result
  return hashed
