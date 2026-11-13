"""JWT verification. This service never issues tokens, it only trusts ones signed with the shared secret."""
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from app.api.errors import Unauthorized

# auto_error=False so *we* raise our own error envelope instead of FastAPI's default.
_bearer = HTTPBearer(auto_error=False)


async def get_current_user_id(
    request: Request,
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> str:
    if creds is None:
        raise Unauthorized("Missing bearer token")
    settings = request.app.state.settings
    try:
        claims = jwt.decode(creds.credentials, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except JWTError as exc:
        raise Unauthorized("Invalid or expired token") from exc
    user_id = claims.get("sub")
    if not user_id:
        raise Unauthorized("Token has no subject")
    return str(user_id)


CurrentUser = Annotated[str, Depends(get_current_user_id)]
