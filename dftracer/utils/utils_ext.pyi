"""Type stubs for utils_ext module."""

def set_log_level(level: str) -> None:
    """Set the global log level using a string (trace, debug, info, warn, error, critical, off)."""
    ...

def set_log_level_int(level: int) -> None:
    """Set the global log level using an integer (0=trace, 1=debug, 2=info, 3=warn, 4=error, 5=critical, 6=off)."""
    ...

def get_log_level_string() -> str:
    """Get the current global log level as a string."""
    ...

def get_log_level_int() -> int:
    """Get the current global log level as an integer."""
    ...