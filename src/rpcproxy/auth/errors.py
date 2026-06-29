"""Authentication module exceptions."""


class AuthError(Exception):
    """Base exception for authentication failures."""


class InvalidApiKeyError(AuthError):
    """Raised when an API key is malformed or fails verification."""


class LoginSessionError(AuthError):
    """Raised when a login session cannot be used."""


class LocalAuthorizationError(AuthError):
    """Raised when local browser authorization fails."""


class LocalAuthorizationServerError(LocalAuthorizationError):
    """Raised when the local authorization server cannot run."""
