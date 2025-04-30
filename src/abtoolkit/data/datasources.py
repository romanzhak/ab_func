from dataclasses import dataclass
from textwrap import shorten
from typing import Dict

@dataclass(frozen=True, slots=True)
class DataSource:
    """
      Meta‑information about tables used in experiments
    """

    path: str
    description: str = ''
    link: str = ''

    def as_row(self, key: str, *, width: int = 35) -> str:
        desc = shorten(self.description, width=50, placeholder='…')
        return f'{key:<15} {self.path:<{width}} {desc:<52} {self.link}'

REFERENCE_SOURCES: Dict[str, DataSource] = {
    'ab_users': DataSource(
        path='abtests.ab_users_data_hs_amp',
        description='Таблица с данными по пользователям в АБ тестах',
        link='https://coda.io/d/Analytics_dl5i6MNS8eI/abtests_suHlNAot',
    ),
    'ab_users_metrics': DataSource(
        path='abtests.ab_all_users_metrics_hs_amp',
        description='Таблица с метриками',
        link='https://coda.io/d/Analytics_dl5i6MNS8eI/abtests_suHlNAot',
    ),
    'sessions': DataSource(
        path='bronze.sessions_hs_amp',
        description='Таблица, которая содержит игровые события о начале и завершении игровой сессии',
        link='https://coda.io/d/CoreData_dM5GlWIWMCq/sessions_suhWC_3Q',
    ),
    'events': DataSource(
        path='bronze.all_events_hs_amp',
        description='Таблица c данными о всех игровых событиях, которые происходят в игре',
        link='https://coda.io/d/CoreData_dM5GlWIWMCq/all-events_susEfi8H',
    ),
    'levels': DataSource(
        path='bronze.levels_hs_amp',
        description='Таблица, которая содержит данные о прохождения уровней игроками',
        link='https://coda.io/d/CoreData_dM5GlWIWMCq/levels_suhplyYQ',
    ),
}

__all__ = [
    'DataSource',
    'REFERENCE_SOURCES'
]