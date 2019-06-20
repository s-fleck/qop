from pathlib import Path

def get_project_root(*args) -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent.joinpath(*args)