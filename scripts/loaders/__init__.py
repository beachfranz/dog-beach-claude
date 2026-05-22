"""State-parks photo loader framework.

`StateParksLoader` (ABC) handles the ~70% scaffolding common across the
existing CDPR / OPRD / WSPRC loaders. Per-state subclasses provide just
the discovery + extraction logic (~80 lines instead of ~400).

See `scripts/loaders/_base.py` for the ABC. Per-state subclasses live
alongside in `scripts/loaders/<source>.py`.
"""
from scripts.loaders._base import StateParksLoader, Photo, ParkInfo

__all__ = ["StateParksLoader", "Photo", "ParkInfo"]
