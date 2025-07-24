"""
Authentication service for Chomp API.
Clean, elegant user management with automatic request.state.user injection and unified @protected decorator.
"""

import json
import secrets
from blake3 import blake3
import jwt
import orjson
from datetime import timedelta
from typing import Optional
from os import environ as env
from fastapi import Request

from ..utils import now, log_error, log_info, log_debug, log_warn
from .. import state
from ..models.user import User
from .. import cache
from ..utils.decorators import service_method
from ..actions.store import store
from ..actions.load import load_resource


# === HELPER FUNCTIONS ===

def generate_uid_from_ip(ip_address: str) -> str:
  """Generate UID from IP address using blake3 hash"""
  return blake3(ip_address.encode()).hexdigest()[:16]


def extract_ip_from_context(request: Optional[Request] = None, websocket=None) -> str:
  """Extract IP address from request or websocket context"""
  if request and hasattr(request, 'client') and request.client:
    return request.client.host
  elif websocket and hasattr(websocket, 'client'):
    return websocket.client.host
  return "127.0.0.1"  # Fallback


@service_method("User creation")
async def get_or_create_user(uid: str, request: Optional[Request] = None,
                           websocket=None, ip_address: Optional[str] = None) -> User:
  """Get or create user with caching and automatic profile creation"""
  # Check cache first
  cached_user = await cache.get_cache(f"user:{uid}", pickled=True)
  if cached_user:
    cached_user.updated_at = now()
    # await cache.cache(f"user:{uid}", cached_user, expiry=86400, pickled=True) # defered to end of req lifecycle
    return cached_user

  # Extract IP if not provided
  if not ip_address:
    ip_address = extract_ip_from_context(request, websocket)

  if state.args.verbose:
    log_debug(f"Creating new user {uid} from IP {ip_address}")

  ts = now()

  # Create user with session details
  user = User(
      uid=uid,
      ipv4=ip_address,
      created_at=ts,
      updated_at=ts,
      status="public",  # Default status
  )

  # Cache new user
  await cache.cache(f"user:{uid}", user, expiry=86400, pickled=True)
  if state.args.verbose:
    log_debug(f"Created new user: {uid} from IP {ip_address}")
  return user


# === REQUEST USER PROPERTY INJECTION ===

async def get_request_user(request: Request) -> Optional['User']:
  """Get user from request context (called by middleware)"""
  # This is called by middleware to populate request.state.user
  if hasattr(request.state, 'user') and request.state.user:
    return request.state.user

  # Load user using AuthService
  user = await AuthService.load_request_user(request)
  request.state.user = user
  return user


class AuthService:
  """Clean authentication service"""

  @staticmethod
  async def load_request_user(request: Request) -> Optional['User']:
    """Load user from request (called by request.state.user property)"""
    try:
      # Try to get authenticated user first
      auth_header = request.headers.get("authorization", "")
      if auth_header.startswith("Bearer "):
        token = auth_header[7:]

        # Check JWT token (primary authentication method)
        try:
          payload = AuthService.verify_jwt_token(token)
          if payload:
            uid = payload.get("user_id")
            if uid:
              log_debug(f"Authenticated user {uid} via JWT")
              # Load authenticated user (this will auto-create if needed)
              return await get_or_create_user(uid, request)
        except Exception as e:
          log_debug(f"JWT verification failed: {e}")

      # Fallback to anonymous user based on IP
      ip = extract_ip_from_context(request)
      uid = generate_uid_from_ip(ip)
      log_debug(f"Creating anonymous user for IP {ip}")
      return await get_or_create_user(uid, request, ip_address=ip)

    except Exception as e:
      log_error(f"User loading failed: {e}")
      return User(uid="anonymous", status="public")

  # === JWT TOKEN METHODS ===

  @staticmethod
  @service_method("JWT generation")
  async def generate_jwt_token(user_id: str, additional_claims: Optional[dict] = None) -> str:
    """Generate JWT token"""
    config = state.server_config
    secret_key = config.jwt_secret_key or env.get("JWT_SECRET_KEY") or secrets.token_urlsafe(32)

    payload = {
      "user_id": user_id,
      "iat": now(),
      "exp": now() + timedelta(hours=config.jwt_expires_hours),
      "iss": "chomp-api"
    }

    if additional_claims:
      payload.update(additional_claims)

    log_info(f"Generated JWT token for user {user_id}")
    return jwt.encode(payload, secret_key, algorithm="HS256")

  @staticmethod
  def verify_jwt_token(token: str) -> Optional[dict]:
    """Verify JWT token"""
    config = state.server_config
    secret_key = config.jwt_secret_key or env.get("JWT_SECRET_KEY") or secrets.token_urlsafe(32)

    try:
      payload = jwt.decode(token, secret_key, algorithms=["HS256"])
      return payload
    except jwt.ExpiredSignatureError:
      log_debug("JWT token expired")
      raise ValueError("Token expired")
    except jwt.InvalidTokenError:
      log_debug("Invalid JWT token")
      raise ValueError("Invalid token")
    except Exception as e:
      log_error(f"JWT verification failed: {e}")
      raise ValueError("Token verification failed")

  # === SESSION MANAGEMENT ===

  @staticmethod
  @service_method("Session storage")
  async def store_session(user: User) -> bool:

    # Renew session (same token)
    if state.server_config.auto_renew_session:
      user.session_expires_at = now() + timedelta(seconds=state.server_config.session_ttl)
    user.updated_at = now()

    # Update user
    await store(user)
    return True

  @staticmethod
  @service_method("Session verification")
  async def verify_and_renew_session(uid: str, token: str) -> bool:
    """Verify and renew session using User object"""
    user = await cache.get_cache(f"user:{uid}", pickled=True)
    if not user or user.jwt_token != token:
      log_warn(f"Invalid session token for user {uid}")
      raise PermissionError("Invalid session")

    # Check if session expired
    if user.session_expires_at and user.session_expires_at < now():
      log_warn(f"Expired session for user {uid}")
      raise PermissionError("Session expired")

    # Auto-renew session
    await AuthService.store_session(user)
    log_debug(f"Renewed session for user {uid}")
    return True

  @staticmethod
  async def get_user(uid: str) -> Optional['User']:
    """Get user by UID (compatibility method)"""
    try:
      cached_user = await cache.get_cache(f"user:{uid}", pickled=True)
      if not cached_user:
        log_debug(f"User {uid} not found in cache")
      return cached_user
    except Exception:
      return None

  @staticmethod
  async def verify_session(user_id: str, token: str) -> bool:
    """Verify session (compatibility method)"""
    return await AuthService.verify_and_renew_session(user_id, token)

  # === CHALLENGE MANAGEMENT ===

  @staticmethod
  @service_method("Challenge creation")
  async def create_auth_challenge(auth_method: str, client_uid: str, identifier: Optional[str] = None) -> dict:
    """Create authentication challenge"""
    config = state.server_config
    if auth_method.lower() not in config.auth_methods:
      log_warn(f"Attempted to create challenge for disabled auth method: {auth_method}")
      raise ValueError(f"{auth_method.upper()} authentication not enabled")

    challenge_id = secrets.token_urlsafe(32)
    expires_at = now() + timedelta(seconds=config.auth_flow_expiry)

    challenge_data = {
      "challenge_id": challenge_id,
      "auth_method": auth_method,
      "identifier": identifier or "user",
      "expires_at": expires_at.isoformat(),
      "client_uid": client_uid,
      "message": f"Welcome to Chomp!\n\nPlease sign this message to authenticate your account.\n\nChallenge ID: {challenge_id}\nAddress: {identifier}\nTimestamp: {expires_at.isoformat()}"
    }

    await state.redis.setex(f"challenge:{challenge_id}", config.auth_flow_expiry, json.dumps(challenge_data))
    log_info(f"Created auth challenge {challenge_id} for method {auth_method} and client {client_uid}")
    return challenge_data

  @staticmethod
  @service_method("Challenge authentication")
  async def authenticate_with_challenge(challenge_id: str, credentials: dict) -> tuple[str, dict]:
    """Authenticate using challenge"""
    try:
      # Get challenge data
      challenge_data = await state.redis.get(f"challenge:{challenge_id}")
      if not challenge_data:
        log_warn(f"Authentication attempted with invalid/expired challenge: {challenge_id}")
        raise ValueError("Invalid or expired challenge")

      challenge_info = orjson.loads(challenge_data.decode() if isinstance(challenge_data, bytes) else challenge_data)
      auth_method = challenge_info.get("auth_method")

      log_info(f"Authenticating challenge {challenge_id} with method {auth_method}")

      # Verify credentials based on auth method
      if auth_method == "static":
        token = credentials.get("token")
        if not token or not isinstance(token, str):
          raise ValueError("Token is required for static authentication")
        is_valid = AuthService.authenticate_static_token(token)
        if is_valid:
          return "static_admin", {"user_id": "static_admin", "provider": "static"}
        else:
          raise ValueError("Invalid static token")

      elif auth_method == "evm":
        address = credentials.get("address")
        signature = credentials.get("signature")
        message = credentials.get("message")
        if not all([address, signature, message]):
          raise ValueError("Address, signature, and message are required for EVM authentication")
        return await AuthService.authenticate_evm_wallet(address, signature, message)

      else:
        log_warn(f"Unsupported authentication method: {auth_method}")
        raise ValueError(f"Unsupported authentication method: {auth_method}")

    except Exception:
      raise

  @staticmethod
  async def _get_or_create_wallet_user(address: str, auth_method: str) -> 'User':
    """Get or create a user for wallet authentication"""
    from ..models.user import User
    from ..actions.store import store

    # Try to find existing user
    user_uid = f"{auth_method}:{address.lower()}"
    existing_user_data = await load_resource("user", uid=user_uid)

    if existing_user_data:
      if isinstance(existing_user_data, dict):
        return User.from_dict(existing_user_data)
      elif isinstance(existing_user_data, User):
        return existing_user_data

    # Create new user
    new_user = User(
        uid=user_uid,
        alias=address[:8] + "..." + address[-4:],  # Shortened address
        status="public"
    )

    await store(new_user, publish=False, monitor=False)
    log_info(f"Created new {auth_method} wallet user: {user_uid}")
    return new_user

  @staticmethod
  def _generate_session_token() -> str:
    """Generate a secure session token"""
    return secrets.token_urlsafe(32)

  @staticmethod
  def authenticate_static_token(token: str) -> bool:
    """Authenticate static token"""
    config = state.server_config
    if not config.static_auth_token:
      log_warn("Static token authentication attempted but no token configured")
      return False

    is_valid = secrets.compare_digest(token, config.static_auth_token)
    if not is_valid:
      log_warn("Invalid static token provided")
    return is_valid

  @staticmethod
  @service_method("Login")
  async def login(uid: str, token: str, auth_method: str) -> str:
    """Login method that handles different authentication methods and creates JWT sessions"""
    log_info(f"Login attempt for user {uid} with method {auth_method}")

    if auth_method.lower() == "static":
      # Validate static token
      if not AuthService.authenticate_static_token(token):
        log_warn(f"Login failed for user {uid}: invalid static token")
        raise ValueError("Invalid static token")

      # Create JWT token for the session
      jwt_token = await AuthService.generate_jwt_token(uid or "static_admin")

      # Store session - get user object first
      user = await get_or_create_user(uid or "static_admin")
      user.jwt_token = jwt_token
      await AuthService.store_session(user)

      log_info(f"Login successful for user {uid}")
      return jwt_token

    else:
      log_error(f"Unsupported authentication method: {auth_method}")
      raise ValueError(f"Authentication method '{auth_method}' not supported")

  @staticmethod
  @service_method("Universal authentication")
  async def universal_authenticate(auth_method: str, credentials: dict) -> tuple[str, dict]:
    """Universal authentication method for direct auth (no challenge required)"""
    log_info(f"Universal authentication attempt with method {auth_method}")

    if auth_method.lower() == "static":
      token = credentials.get("token")
      if not token:
        log_warn("Universal auth failed: missing token in credentials")
        raise ValueError("Missing token in credentials")

      jwt_token = await AuthService.login("static_admin", token, "static")

      user_data = {
        "user_id": "static_admin",
        "provider": "static",
        "username": "admin"
      }

      log_info("Universal authentication successful for static admin")
      return jwt_token, user_data

    else:
      log_error(f"Unsupported authentication method: {auth_method}")
      raise ValueError(f"Authentication method '{auth_method}' not supported")

  @staticmethod
  @service_method("EVM wallet authentication")
  async def authenticate_evm_wallet(address: str, signature: str, message: str) -> tuple[str, dict]:
    """Authenticate using EVM wallet signature"""
    try:
      from web3 import Web3
      from eth_account.messages import encode_defunct

      # Verify the signature
      message_hash = encode_defunct(text=message)
      recovered_address = Web3().eth.account.recover_message(message_hash, signature=signature)

      if recovered_address.lower() != address.lower():
        log_warn(f"EVM wallet signature verification failed for address: {address}")
        raise ValueError("Invalid signature")

      # Create or get user
      user = await AuthService._get_or_create_wallet_user(address, "evm")

      # Generate session token
      session_token = AuthService._generate_session_token()

      # Store session
      await state.redis.setex(f"session:{session_token}",
                              state.server_config.session_duration_seconds,
                              orjson.dumps(user.to_dict()))

      log_info(f"EVM wallet authentication successful for address: {address}")
      return session_token, user.to_dict()

    except Exception:
      raise

  # === UTILITY METHODS ===

  @staticmethod
  def client_uid(request: Request) -> str:
    """Get requester ID from request"""
    try:
      user = request.state.user
      return user.uid if user else "anonymous"
    except Exception:
      ip = extract_ip_from_context(request)
      return generate_uid_from_ip(ip)

  @staticmethod
  async def load_websocket_user(websocket) -> Optional['User']:
    """Get or create user from websocket connection, checking query params and headers."""
    try:
      # Try token from query params first (most common for websockets)
      token = websocket.query_params.get("token")

      # Fallback to Authorization header
      if not token:
        auth_header = websocket.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
          token = auth_header[7:]

      # Verify JWT if present
      if token:
        try:
          payload = AuthService.verify_jwt_token(token)
          if payload and (user_id := payload.get("user_id")):
            log_debug(f"Authenticated websocket user {user_id} via JWT")
            return await get_or_create_user(user_id, websocket=websocket)
        except Exception as e:
          log_debug(f"Websocket JWT verification failed: {e}")

      # Fallback to anonymous user based on IP
      ip = extract_ip_from_context(websocket=websocket)
      uid = generate_uid_from_ip(ip)
      log_debug(f"Creating anonymous websocket user for IP {ip}")
      return await get_or_create_user(uid, websocket=websocket, ip_address=ip)

    except Exception as e:
      log_error(f"User loading from websocket failed: {e}")
      return User(uid="anonymous", status="public")

  @staticmethod
  @service_method("Logout")
  async def logout(user_id: str) -> bool:
    """Logout user by invalidating session"""
    try:
      await state.redis.delete(f"session:{user_id}")
      await state.redis.delete(f"user:{user_id}")
      log_info(f"User {user_id} logged out successfully")
      return True
    except Exception:
      raise

  @staticmethod
  @service_method("Get challenge status")
  async def get_challenge_status(challenge_id: str) -> dict:
    """Get challenge status"""
    challenge_data = await state.redis.get(f"challenge:{challenge_id}")
    if not challenge_data:
      log_warn(f"Challenge status requested for non-existent challenge: {challenge_id}")
      raise ValueError("Challenge not found or expired")

    return orjson.loads(challenge_data.decode() if isinstance(challenge_data, bytes) else challenge_data)

  @staticmethod
  @service_method("Cancel challenge")
  async def cancel_challenge(challenge_id: str) -> bool:
    """Cancel challenge"""
    deleted = await state.redis.delete(f"challenge:{challenge_id}")
    if deleted > 0:
      log_info(f"Cancelled challenge {challenge_id}")
    else:
      log_warn(f"Attempted to cancel non-existent challenge: {challenge_id}")
    return deleted > 0
