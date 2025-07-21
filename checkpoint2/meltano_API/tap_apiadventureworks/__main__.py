"""APIAdventureWorks entry point."""

from __future__ import annotations

from tap_apiadventureworks.tap import TapAPIAdventureWorks

TapAPIAdventureWorks.cli()
