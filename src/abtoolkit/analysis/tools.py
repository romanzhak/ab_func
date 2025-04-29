from ..config import ResearchConfig, Source
import pyspark.sql.functions as F
from databricks.sdk.runtime import spark

def verify_ab_exp(cfg: ResearchConfig) -> None:
  ab_test_users = spark.table(Source.AB_USERS.path)
  ab_users = ab_test_users.filter((F.col('test_id') == cfg.test_id) & (F.col('app_short') == cfg.app_short))
  test_info = (
    ab_users.select(
        'test_id',
        'app_short',
        'test_start_date',
        'test_end_date',
        'enroll_end_date',
        'feature_start_date',
        'feature_end_date'
    )
    .distinct()
  )
  test_info.display()