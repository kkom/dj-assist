import os


def getenvx(name: str) -> str:
    value = os.getenv(name)

    if value is None:
        raise ValueError(f"Missing {name} environment value")

    return value
