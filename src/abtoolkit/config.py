'''abtoolkit.config
===================
Centralised configuration object for A/B‑test analyses.
Use `ResearchConfig` to pass experiment‑specific parameters to any
function within the library and to keep temporary Spark table
names consistent.
'''
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, Any
from .datasources import REFERENCE_SOURCES, DataSource

version = '0.1.0'

class Platform(str, Enum):
    '''
      Enumeration of the supported game platforms
      Includes only 2: ios and gp
    '''

    IOS = 'iOS'
    ANDROID = 'Android'

    @property
    def app_short(self) -> str:
        '''Return short application code used in internal table names'''
        return 'hs_as' if self is Platform.IOS else 'hs_gp'

    @property
    def store(self) -> str:
        '''Return canonical store identifier'''
        return 'ios' if self is Platform.IOS else 'googleplay'


class Source(str, Enum):
    '''
    Canonical data sources.

    Example
    -------
    >>> from abtoolkit.config import Source
    >>> Source.EVENTS.path
    '''

    AB_USERS = 'ab_users'
    AB_USERS_METRICS = 'ab_users_metrics'

    def _ds(self) -> DataSource:
        try:
            return REFERENCE_SOURCES[self.value]
        except KeyError as exc:
            raise KeyError(
                f'DataSource "{self.value}" missing in REFERENCE_SOURCES registry'
            ) from exc

    @property
    def path(self) -> str:
        return self._ds().path

    @property
    def description(self) -> str:
        return self._ds().description

    @property
    def link(self) -> str:
        return self._ds().link

    @classmethod
    def print_catalog(cls) -> None:
        """Print each data source in a multi‑line compact form."""
        for member in cls:
            ds = member._ds()
            print(f"{member.name.lower()}:")
            print(f"  path: {ds.path}")
            if ds.description:
                print(f"  desc: {ds.description}")
            if ds.link:
                print(f"  link: {ds.link}")
            print()

# ---------------------------------------------------------------------------
# Research‑specific configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ResearchConfig:
    '''
    Parameters of a single A/B‑test.

    Stores only experiment‑specific data; relies on :class:`Source` enum
    for actual table locations.
    '''

    your_name: str
    test_id: str
    platform: Platform = Platform.IOS
    alpha: float = 0.05
    beta: float = 0.2
    meta: Dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @property
    def app_short(self) -> str:
        return self.platform.app_short

    @property
    def store(self) -> str:
        return self.platform.store

    @property
    def dbname(self) -> str:
        return (
            f'game_data_prod.temp.hs_{self.your_name}_abtests_metrics_'
            f'{self.test_id}_{self.app_short}'
        )
    
    # special

    def add_meta(self, key: str, value: Any, *, overwrite: bool = True) -> None:
        """Store *value* under *key* in :pyattr:`meta`.

        Parameters
        ----------
        key
            Dot‑notation allowed (e.g. ``"dates.test_start_date"``) — nested
            dicts will be created automatically.
        value
            Any serialisable object (Spark objects *not* recommended).
        overwrite
            If *False*, raising :class:`ValueError` when *key* already exists.
        """
        parts = key.split(".")
        target: Mapping[str, Any] | Dict[str, Any] = self.meta
        for p in parts[:-1]:
            target = target.setdefault(p, {})  # type: ignore[arg-type]
        final_key = parts[-1]
        if not overwrite and final_key in target:
            raise ValueError(
                f"meta['{key}'] already exists; set overwrite=True to replace."
            )
        target[final_key] = value  # type: ignore[index]

    def get_meta(self, key: str, default: Any | None = None) -> Any:
        """Retrieve value by *key* (dot‑notation supported)."""
        parts = key.split(".")
        current: Any = self.meta
        for p in parts:
            if not isinstance(current, dict):
                return default
            if p not in current:
                return default
            current = current[p]
        return current

    def ingest_dates(self, df: Any, *, overwrite: bool = True) -> None:  # type: ignore[name-defined]
        """Populate ``meta['dates']`` from a one‑row Spark or pandas DataFrame.

        The *df* must contain **exactly one row** and columns
        ``test_start_date``, ``test_end_date``, ``enroll_end_date``,
        ``feature_start_date``, ``feature_end_date``.  Missing columns raise
        :class:`ValueError`.
        """
        required = {
            "test_start_date",
            "test_end_date",
            "enroll_end_date",
            "feature_start_date",
            "feature_end_date",
        }
        cols = set(df.columns)
        missing = required - cols
        if missing:
            raise ValueError(f"DataFrame missing required columns: {sorted(missing)}")
        if df.count() if hasattr(df, "count") else len(df) != 1:  # Spark or pandas len
            raise ValueError("DataFrame must contain exactly one row with test metadata")
        row = df.collect()[0] if hasattr(df, "collect") else df.iloc[0]
        self.add_meta("dates", {c: getattr(row, c) for c in required}, overwrite=overwrite)

__all__ = [
    'Platform',
    'Source',
    'ResearchConfig',
]
