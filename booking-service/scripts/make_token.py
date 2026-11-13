"""Mint a dev JWT (auth-service will do this later).   Run:  python -m scripts.make_token alice"""
import sys
from datetime import timedelta

from jose import jwt

from app.core.config import get_settings
from app.domain.models import utcnow

settings = get_settings()
user = sys.argv[1] if len(sys.argv) > 1 else "alice"
token = jwt.encode({"sub": user, "exp": utcnow() + timedelta(days=1)}, settings.jwt_secret, algorithm=settings.jwt_algorithm)
print(token)
