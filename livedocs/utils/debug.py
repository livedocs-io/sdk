import json
from typing import Any

from IPython.display import display


def debug(label: str, data: Any):
    """Sends data to the middleman for pretty-printing in its logs."""
    try:
        content_str = json.dumps(data, indent=2, default=str)
        mime_type = "application/json"
    except Exception:
        content_str = str(data)
        mime_type = "text/plain"
    display(
        {mime_type: content_str},
        metadata={
            "middleman_debug": True,
            "middleman_debug_label": label,
        },
        raw=True,
    )
