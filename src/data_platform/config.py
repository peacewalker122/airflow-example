import os


def setting(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or not value.strip():
        raise ValueError(f"{name} must be configured")
    return value.strip()


def positive_int_setting(name: str, default: int) -> int:
    value = int(setting(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be greater than zero")
    return value
