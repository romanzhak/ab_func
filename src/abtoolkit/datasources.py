from dataclasses import dataclass, field, asdict
from textwrap import shorten
from typing import Dict, Any

@dataclass(frozen=True, slots=True)
class DataSource:
    '''
      Meta‑information about tables used in experiments
    '''

    path: str
    description: str = ''
    link: str = ''

    def as_row(self, key: str, *, width: int = 35) -> str:
        desc = shorten(self.description, width=50, placeholder='…')
        return f'{key:<15} {self.path:<{width}} {desc:<52} {self.link}'

REFERENCE_SOURCES: Dict[str, DataSource] = {
    'ab_users': DataSource(
        path="abtests.ab_users_data_hs_amp",
        description='Таблица с данными по пользователям в АБ тестах',
        link='https://coda.io/d/Analytics_dl5i6MNS8eI/abtests_suHlNAot',
    ),
    'ab_users_metrics': DataSource(
        path="abtests.ab_all_users_metrics_hs_amp",
        description='Таблица с метриками',
        link='https://coda.io/d/Analytics_dl5i6MNS8eI/abtests_suHlNAot',
    ),
}