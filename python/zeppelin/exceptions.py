"""Zeppelin client exceptions."""


class ZeppelinError(Exception):
    """Base exception for Zeppelin client errors."""

    def __init__(self, message: str, status_code: int | None = None):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class NotFoundError(ZeppelinError):
    """Resource not found (HTTP 404)."""


class ConflictError(ZeppelinError):
    """Resource conflict (HTTP 409)."""


class ValidationError(ZeppelinError):
    """Invalid request (HTTP 400)."""


class ServerError(ZeppelinError):
    """Server-side error (HTTP 5xx)."""
