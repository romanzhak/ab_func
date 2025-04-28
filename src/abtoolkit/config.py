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
                f'DataSource "{self.value}" missing in _REFERENCE_SOURCES registry'
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
        header = f'{"Key":<15} {"Path":<35} {"Description":<52} Link'
        print(header)
        print('-' * len(header))
        for member in cls:
            print(member._ds().as_row(member.name.lower()))

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

__all__ = [
    'Platform',
    'Source',
    'DataSource',
    'ResearchConfig',
]
