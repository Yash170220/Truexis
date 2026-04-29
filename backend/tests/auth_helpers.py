"""Test helpers: bypass JWT on the shared FastAPI app."""
import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock

from src.api.auth import get_current_user


def _mock_user():
    m = MagicMock()
    m.id = uuid.uuid4()
    m.is_active = True
    m.email = "test@example.com"
    m.name = "Test User"
    return m


def attach_mock_auth_user(app) -> None:
    app.dependency_overrides[get_current_user] = _mock_user


@contextmanager
def app_auth_patched(app):
    prev = app.dependency_overrides.get(get_current_user)
    attach_mock_auth_user(app)
    try:
        yield
    finally:
        if prev is not None:
            app.dependency_overrides[get_current_user] = prev
        else:
            app.dependency_overrides.pop(get_current_user, None)
