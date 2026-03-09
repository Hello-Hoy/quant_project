class QuantProjectError(Exception):
    """Base exception for quant project scaffold."""


class ConfigError(QuantProjectError):
    """Raised when app configuration is invalid or missing."""


class ProviderNotImplementedError(QuantProjectError):
    """Raised when a provider integration is intentionally not implemented yet."""
