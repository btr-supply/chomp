from hashlib import md5
from fastapi import FastAPI, Request, HTTPException, Response
from .. import state

# TODO: implement pattern/geo tracking to flag rotating proxies
_id_cache = {}
def requester_id(req: Request, id_salt: str="reqid:") -> str:
  ip = req.client.host
  if ip in _id_cache:
    return _id_cache[ip]
  salted = f"{id_salt}{ip}".encode('utf-8')
  hashed = md5(salted).hexdigest()
  _id_cache[ip] = hashed # cache the result
  return hashed

def requester_id(req: Request) -> str:
  """Get requester ID from request"""
  return req.state.requester_id if hasattr(req.state, 'requester_id') else req.client.host
