"""
abtoolkit.config
===================
Centralised configuration object for A/B‑test analyses.
Use `ResearchConfig` to pass experiment‑specific parameters to any
function within the library and to keep temporary Spark table
names consistent.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any
from .datasources import REFERENCE_SOURCES, DataSource

version = '0.1.0'

class Platform(str, Enum):
    """
      Enumeration of the supported game platforms
      Includes only 2: ios and gp
    """

    IOS = 'iOS'
    ANDROID = 'Android'

    @property
    def app_short(self) -> str:
        """Return short application code used in internal table names"""
        return 'hs_as' if self is Platform.IOS else 'hs_gp'

    @property
    def store(self) -> str:
        """Return canonical store identifier"""
        return 'ios' if self is Platform.IOS else 'googleplay'


class Source(str, Enum):
    """
    Canonical data sources.

    Example
    -------
    >>> from abtoolkit.config import Source
    >>> Source.EVENTS.path
    """

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
            print(f'{member.name.lower()}:')
            print(f'  path: {ds.path}')
            if ds.description:
                print(f'  desc: {ds.description}')
            if ds.link:
                print(f'  link: {ds.link}')
            print()

# ---------------------------------------------------------------------------
# Research‑specific configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ResearchConfig:
    """
    Parameters of a single A/B‑test.

    Stores only experiment‑specific data; relies on :class:`Source` enum
    for actual table locations.
    """

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
        """Save *value* in :pyattr:`meta` under *key*.

        Parameters
        ----------
        key
            Simple string
        value
            Any serialisable object.
        overwrite
            If *False*, raise :class:`ValueError` when *key* already exists.
        """
        if not overwrite and key in self.meta:
            raise ValueError(f'meta["{key}"] already exists; set overwrite=True to replace.')
        self.meta[key] = value

    def get_meta(self, key: str, default: Any | None = None) -> Any:
        """Return value stored in :pyattr:`meta` or *default*."""
        return self.meta.get(key, default)

    def ingest_fields(self, df: Any, required_fileds: dict, *, overwrite: bool = True) -> None:
        """
           Extract key dates from a one‑row DataFrame and stash into :pyattr:`meta`.
        """
        cols = set(df.columns)
        missing = required_fileds - cols
        if missing:
            raise ValueError(f'DataFrame missing required_fileds columns: {sorted(missing)}')
        n_rows = df.count() if hasattr(df, 'count') else len(df)
        if n_rows != 1:
            raise ValueError('DataFrame must contain exactly one row with test metadata')
        row = df.collect()[0] if hasattr(df, 'collect') else df.iloc[0]
        for col in required_fileds:
            self.add_meta(col, getattr(row, col), overwrite=overwrite)

__all__ = [
    'Platform',
    'Source',
    'ResearchConfig',
]
