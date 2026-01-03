import hashlib
from typing import Optional, Any

def create_item_deduplication_id(
    *,
    place_id: Optional[str] = None,          # models.py uses this
    google_place_id: Optional[str] = None,   # keep compatible too
    source_platform: Optional[str] = None,
    source_id: Optional[str] = None,
    is_event: Optional[bool] = None,
    **kwargs: Any,                           # accept future extra args safely
) -> Optional[str]:
    """
    Compatible local implementation.

    The upstream code may call this with place_id or google_place_id.
    We accept both, plus extra kwargs, to avoid breaking validation.
    """
    pid = place_id or google_place_id

    if pid:
        base = f"pid:{pid}"
    elif source_platform and source_id:
        base = f"src:{source_platform}:{source_id}"
    else:
        return None

    if is_event is not None:
        base += f"|is_event:{int(bool(is_event))}"

    return hashlib.md5(base.encode("utf-8")).hexdigest()  # 32 chars

