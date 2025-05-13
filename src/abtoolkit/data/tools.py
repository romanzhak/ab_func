from .datasources import REFERENCE_SOURCES, DataSource
from enum import Enum

class Source(str, Enum):
    """
    Canonical data sources.

    Example
    -------
    >>> from abtoolkit.data.tools import Source
    >>> Source.EVENTS.path
    """

    AB_USERS = 'ab_users'
    AB_USERS_METRICS = 'ab_users_metrics'
    SESSIONS = 'sessions'
    EVENTS = 'events'
    LEVELS = 'levels'

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
            print(f'{member.name}:')
            print(f'  path: {ds.path}')
            if ds.description:
                print(f'  desc: {ds.description}')
            if ds.link:
                print(f'  link: {ds.link}')
            print()

__all__ = [
    'Source',
]