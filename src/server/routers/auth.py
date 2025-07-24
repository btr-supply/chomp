"""
Authentication router for Chomp API.
Clean endpoints for login, challenge flow, and session management.
"""

from fastapi import APIRouter, Request, HTTPException, Body
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

from ...server.responses import ApiResponse
from ..routes import Route
from ...services.auth import AuthService
from ...utils import now, log_info

from .. import state


router = APIRouter(tags=["auth"])


# Request/Response Models
class AuthRequest(BaseModel):
  auth_method: str = Field(..., description="Authentication method (static, evm, svm, sui)")
  credentials: Dict[str, Any] = Field(..., description="Method-specific credentials")


class ChallengeRequest(BaseModel):
  auth_method: str = Field(..., description="Authentication method")
  identifier: Optional[str] = Field(None, description="User identifier (e.g., wallet address)")


class ChallengeResponse(BaseModel):
  challenge_id: str
  auth_method: str
  message: str
  expires_at: str


class AuthChallengeRequest(BaseModel):
  challenge_id: str = Field(..., description="Challenge ID from create_challenge")
  credentials: Dict[str, Any] = Field(..., description="Authentication credentials")


class AuthResponse(BaseModel):
  access_token: str
  token_type: str = "bearer"
  user: Dict[str, Any]


class LoginRequest(BaseModel):
  user_id: Optional[str] = None
  token: str
  auth_method: str


# === AUTHENTICATION ENDPOINTS ===

@router.post(Route.AUTH_LOGIN.endpoint, response_model=AuthResponse)
async def login(request: Request, login_data: LoginRequest) -> AuthResponse:
  """Direct login endpoint (simplified authentication)"""
  client_uid = AuthService.client_uid(request)
  log_info(f"Login attempt from {client_uid} using {login_data.auth_method}")

  token = await AuthService.login(
      user_id=login_data.user_id,
      token=login_data.token,
      auth_method=login_data.auth_method
  )

  user_data = {
      "user_id": login_data.user_id or "static_admin",
      "provider": login_data.auth_method,
      "username": (login_data.user_id or "static_admin").split(":")[-1]
  }

  log_info(f"Login successful for {client_uid}")
  return AuthResponse(access_token=token, user=user_data)


@router.post(Route.AUTH_AUTHENTICATE.endpoint, response_model=AuthResponse)
async def authenticate(request: Request, auth_data: AuthRequest) -> AuthResponse:
  """Universal authentication endpoint"""
  client_uid = AuthService.client_uid(request)
  log_info(f"Auth attempt from {client_uid} using {auth_data.auth_method}")

  token, user_data = await AuthService.universal_authenticate(
      auth_method=auth_data.auth_method,
      credentials=auth_data.credentials
  )

  log_info(f"Authentication successful for {client_uid}")
  return AuthResponse(access_token=token, user=user_data)


# === CHALLENGE FLOW ===

@router.post(Route.AUTH_CHALLENGE_CREATE.endpoint, response_model=ChallengeResponse)
async def create_challenge(request: Request, challenge_req: ChallengeRequest) -> ChallengeResponse:
  """Create authentication challenge"""
  client_uid = AuthService.client_uid(request)

  challenge_data = await AuthService.create_auth_challenge(
      auth_method=challenge_req.auth_method,
      client_uid=client_uid,
      identifier=challenge_req.identifier
  )

  return ChallengeResponse(
      challenge_id=challenge_data["challenge_id"],
      auth_method=challenge_data["auth_method"],
      message=challenge_data["message"],
      expires_at=challenge_data["expires_at"]
  )


@router.post(Route.AUTH_CHALLENGE_AUTH.endpoint, response_model=AuthResponse)
async def authenticate_challenge(request: Request, auth_req: AuthChallengeRequest) -> AuthResponse:
  """Authenticate using challenge"""
  token, user_data = await AuthService.authenticate_with_challenge(
      challenge_id=auth_req.challenge_id,
      credentials=auth_req.credentials
  )

  return AuthResponse(access_token=token, user=user_data)


@router.get(Route.AUTH_CHALLENGE_STATUS.endpoint)
async def get_challenge_status(request: Request, challenge_id: str) -> ApiResponse:
  """Get challenge status"""
  status = await AuthService.get_challenge_status(challenge_id)
  return ApiResponse(status)


@router.delete(Route.AUTH_CHALLENGE_CANCEL.endpoint)
async def cancel_challenge(request: Request, challenge_id: str) -> ApiResponse:
  """Cancel challenge"""
  success = await AuthService.cancel_challenge(challenge_id)
  return ApiResponse({
      "success": success,
      "message": "Challenge cancelled" if success else "Challenge not found"
  })


# === SESSION MANAGEMENT ===

@router.post(Route.AUTH_LOGOUT.endpoint)
async def logout(request: Request, user_id: str = Body(...)) -> ApiResponse:
  """Logout user"""
  success = await AuthService.logout(user_id)
  return ApiResponse({
      "success": success,
      "message": "Logged out successfully"
  })


# === PROFILE & STATUS ===

@router.get(Route.AUTH_PROFILE.endpoint)
async def get_profile(request: Request) -> ApiResponse:
  """Get user profile"""
  user = request.state.user

  if not user:
    raise HTTPException(status_code=401, detail="Not authenticated")

  return ApiResponse({
      "user": {
          "uid": user.uid,
          "status": user.status,
          "is_admin": user.is_admin(),
          "auth_methods": getattr(user, 'auth_methods', [])
      },
      "timestamp": now().isoformat()
  })


@router.get(Route.AUTH_STATUS.endpoint)
async def auth_status(request: Request) -> ApiResponse:
  """Get authentication status"""
  user = request.state.user
  config = state.server_config

  return ApiResponse({
      "authenticated": bool(user and user.uid != "anonymous"),
      "user_id": user.uid if user else None,
      "user_status": user.status if user else None,
      "is_admin": user.is_admin() if user else False,
      "auth_methods": config.auth_methods,
      "auth_enabled": config.auth_enabled,
      "timestamp": now().isoformat()
  })


# === HEALTH CHECK ===

@router.get(Route.AUTH_HEALTH.endpoint)
async def auth_health() -> ApiResponse:
  """Authentication service health check"""
  config = state.server_config

  return ApiResponse({
      "status": "healthy",
      "auth_enabled": config.auth_enabled,
      "available_methods": config.auth_methods,
      "jwt_configured": bool(config.jwt_secret_key),
      "timestamp": now().isoformat()
  })
