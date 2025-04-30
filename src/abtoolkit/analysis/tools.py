from ..config import ResearchConfig
from ..data.tools import Source
from databricks.sdk.runtime import spark
import pyspark.sql.functions as F

def verify_ab_exp(cfg: ResearchConfig) -> None:
  """
   Checking whether there is a test in the data mart
  """
  ab_test_users = spark.table(Source.AB_USERS.path)
  ab_users = ab_test_users.filter((F.col('test_id') == cfg.test_id) & (F.col('app_short') == cfg.app_short))
  required = {
      'test_start_date',
      'test_end_date',
      'enroll_end_date',
      'feature_start_date',
      'feature_end_date',
  }
  test_info = (
    ab_users.select(
        'test_id',
        'app_short',
        *required,
    )
    .distinct()
  )
  cfg.ingest_fields(test_info, required)
  test_info.display()

__all__ = [
    'verify_ab_exp',
]
