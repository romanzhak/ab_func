from abtoolkit.config import ResearchConfig
from abtoolkit.analysis.stat_tests import (
    relative_ttest,
    relative_cuped,
    linearisation, 
    no_test_ratio
)
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window
from pyspark.sql.types import *
from databricks.sdk.runtime import spark 
import pandas as pd

def compare_metrics(cfg: ResearchConfig, control_group_name: str, test_group_name: str, metric_set: str = 'basic', dates: str = 'personal', n_day: int = 14, significance_level: float = 0.05) -> DataFrame:

    """
    Функция сравнивает метрики между группами теста на заданный день и для каждой метрики применяет один из стат. тестов для определения значимости.
    """

    if dates == 'calendar':
        metrics_data = (spark.table(cfg.get_meta('abtests_metrics_base'))
            .filter(F.col('abgroup').isin([control_group_name, test_group_name]))
            .filter(col('calendar_day') == n_day)
            ).toPandas()
    else:
        metrics_data = (spark.table(cfg.get_meta('abtests_metrics_base'))
            .filter(F.col('abgroup').isin([control_group_name, test_group_name]))
            .filter(col('personal_day') == n_day)
            ).toPandas()

    if metric_set == 'basic':

        """
        arpu            revenue_cum / total_users
        arppu           revenue_cum / converted_users
        darpu           revenue      / retained_users
        darppu          revenue      / paying_users
        conversion      converted_users / total_users 
        paying_share    paying_users  / retained_users
        retention       retained_users / total_users 
        churn           churned_users / total_users 
        """

        comparison_results = []

        # linearisation metrics 

        linear_target_metrics = ['arppu', 'darpu', 'darppu', 'paying share']
        linear_num_metrics = ['revenue_cum', 'revenue', 'revenue', 'converted']
        linear_denom_metrics = ['converted_cum', 'retained', 'converted', 'retained']

        for target_metric_name, num_metric_name, denom_metric_name in zip(linear_target_metrics, linear_num_metrics, linear_denom_metrics):
            result = linearisation(
                          data = metrics_data[['abgroup', 'event_user', num_metric_name, denom_metric_name]]
                        , control_group_name = control_group_name
                        , test_group_name = test_group_name
                        , numerator_metric_name = num_metric_name
                        , denominator_metric_name = denom_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

        # ttest metrics 

        ttest_target_metrics = ['retention', 'conversion', 'churn']
        ttest_user_metrics = ['retained', 'converted_cum', 'churn']

        for target_metric_name, user_metric_name in zip(ttest_target_metrics, ttest_user_metrics):
            result = relative_ttest(
                              data = metrics_data[['abgroup', 'event_user', user_metric_name]]
                            , control_group_name = control_group_name
                            , test_group_name = test_group_name
                            , metric_name = user_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

        # #arpu (cuped)
        result = relative_cuped(
                          data = metrics_data[['abgroup', 'event_user', 'revenue_cum', 'revenue_before_d14']]
                        , control_group_name = control_group_name
                        , test_group_name = test_group_name
                        , metric_name = 'revenue_cum'
                        , metric_before_name = 'revenue_before_d14'
        )
        result.metric = 'arpu'
        comparison_results.append(result)

    elif metric_set == 'match3':

        """
        cum_attempts                        mean(cum_user_attempts)
        cum_wins                            mean(cum_user_wins)
        cum_coins_spent                     mean(cum_user_coins_spent)
        cum_real_coins_spent                mean(cum_user_real_coins_spent)
        cum_real_coins_spent_1att           mean(cum_user_real_coins_spent_1att)
        cum_real_coins_spent_2plus_att      mean(cum_user_real_coins_spent_2plus_att)
        cum_real_coins_spent_superball      mean(cum_user_real_coins_spent_superball)

        dwins                               sum(wins) / sum(retained)
        dattempts                           sum(attempts) / sum(retained)
        daily_share_stucked                 sum(is_stucked) / sum(retained)
        daily_share_super_stucked           sum(is_super_stucked) / sum(retained)
        daily_coins_spent                   sum(coins_spent) / sum(retained)
        daily_real_coins_spent              sum(real_coins_spent) / sum(retained)
        daily_real_coins_spent_1att         sum(real_coins_spent_1att) / sum(retained)
        daily_real_coins_spent_2plus_att    sum(real_coins_spent_2plus_att) / sum(retained)
        daily_churn_m3                      sum(churn_m3) / sum(retained)
        daily_churn_m3_lose                 sum(churn_m3_lose) / sum(retained)
        daily_churn_m3_win                  sum(churn_m3_win) / sum(retained)
        daily_share_rcoins_1att             sum(real_coins_spent_1att) / sum(real_coins_spent)
        daily_share_coins_1att              sum(coins_spent_1att) / sum(coins_spent)
        cum_share_rcoins_1att               sum(cum_user_real_coins_spent_1att) / sum(cum_user_real_coins_spent)
        """

        comparison_results = []

        # ttest metrics 

        ttest_target_metrics = ['cum_attempts', 'cum_wins', 'cum_coins_spent', 'cum_real_coins_spent', 'cum_real_coins_spent_1att', 'cum_real_coins_spent_2plus_att', 'cum_real_coins_spent_superball']
        ttest_user_metrics = ['cum_user_attempts', 'cum_user_wins', 'cum_user_coins_spent', 'cum_user_real_coins_spent', 'cum_user_real_coins_spent_1att', 'cum_user_real_coins_spent_2plus_att', 'cum_user_real_coins_spent_superball']

        for target_metric_name, user_metric_name in zip(ttest_target_metrics, ttest_user_metrics):
            result = relative_ttest(
                              data = metrics_data[['abgroup', 'event_user', user_metric_name]]
                            , control_group_name = control_group_name
                            , test_group_name = test_group_name
                            , metric_name = user_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

        # linearisation metrics 

        linear_target_metrics = ['dwins', 'dattempts', 'daily_share_stucked', 'daily_share_super_stucked', 'daily_coins_spent', 'daily_real_coins_spent', 'daily_real_coins_spent_1att', 'daily_real_coins_spent_2plus_att', 'daily_churn_m3', 'daily_churn_m3_lose', 'daily_churn_m3_win']
        linear_num_metrics = ['wins', 'attempts', 'is_stucked', 'is_super_stucked', 'coins_spent', 'real_coins_spent', 'real_coins_spent_1att', 'real_coins_spent_2plus_att', 'churn_m3', 'churn_m3_lose', 'churn_m3_win']
        linear_denom_metrics = ['retained', 'retained', 'retained', 'retained', 'retained', 'retained', 'retained', 'retained', 'retained', 'retained', 'retained']

        for target_metric_name, num_metric_name, denom_metric_name in zip(linear_target_metrics, linear_num_metrics, linear_denom_metrics):
            result = linearisation(
                          data = metrics_data[['abgroup', 'event_user', num_metric_name, denom_metric_name]]
                        , control_group_name = control_group_name
                        , test_group_name = test_group_name
                        , numerator_metric_name = num_metric_name
                        , denominator_metric_name = denom_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

        # no test metrics 

        linear_target_metrics = ['daily_share_rcoins_1att', 'daily_share_coins_1att', 'cum_share_rcoins_1att']
        linear_num_metrics = ['real_coins_spent_1att', 'coins_spent_1att', 'cum_user_real_coins_spent_1att']
        linear_denom_metrics = ['real_coins_spent', 'coins_spent', 'cum_user_real_coins_spent']

        for target_metric_name, num_metric_name, denom_metric_name in zip(linear_target_metrics, linear_num_metrics, linear_denom_metrics):
            result = no_test_ratio(
                          data = metrics_data[['abgroup', 'event_user', num_metric_name, denom_metric_name]]
                        , control_group_name = control_group_name
                        , test_group_name = test_group_name
                        , numerator_metric_name = num_metric_name
                        , denominator_metric_name = denom_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

    elif metric_set == 'streak':

        """
        cum_cnt_superball             mean(cum_user_cnt_superball)
        cum_cnt_lose_sb               mean(cum_user_cnt_lose_sb)

        daily_avg_length_sb_streak    sum(daily_avg_length_sb_streak) / sum(is_superball) 
        share_sb_users                sum(is_superball) / sum(is_m3)  
        share_not_sb_not_streak       sum(is_not_sb_not_streak) / sum(is_m3) 
        share_streak_users            sum(is_streak) / sum(is_m3)  
        daily_share_users_5plus_ws    sum(is_5plus_ws) / sum(is_m3)  
        daily_share_users_10plus_ws   sum(is_10plus_ws) / sum(is_m3)  
        daily_share_users_20plus_ws   sum(is_20plus_ws) / sum(is_m3)  
        daily_share_users_40plus_ws   sum(is_40plus_ws) / sum(is_m3)  
        share_lost_sb_users           sum(is_lose_sb) / sum(is_superball) 

        no test metrics:
        daily_share_rcoins_1att       sum(real_coins_spent_1att) / sum(real_coins_spent)
        daily_share_rcoins_superball  sum(real_coins_spent_superball) / sum(real_coins_spent)
        daily_share_coins_1att        sum(coins_spent_1att) / sum(coins_spent)
        """

        comparison_results = []

        # ttest metrics 

        ttest_target_metrics = ['cum_cnt_superball', 'cum_cnt_lose_sb']
        ttest_user_metrics = ['cum_user_cnt_superball', 'cum_user_cnt_lose_sb']

        for target_metric_name, user_metric_name in zip(ttest_target_metrics, ttest_user_metrics):
            result = relative_ttest(
                              data = metrics_data[['abgroup', 'event_user', user_metric_name]]
                            , control_group_name = control_group_name
                            , test_group_name = test_group_name
                            , metric_name = user_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

        # linearisation metrics 

        linear_target_metrics = ['daily_avg_length_sb_streak', 'share_sb_users', 'share_not_sb_not_streak', 'share_streak_users', 'daily_share_users_5plus_ws', 'daily_share_users_10plus_ws', 'daily_share_users_20plus_ws', 'daily_share_users_40plus_ws', 'share_lost_sb_users']
        linear_num_metrics = ['daily_avg_length_sb_streak', 'is_superball', 'is_not_sb_not_streak', 'is_streak', 'is_5plus_ws', 'is_10plus_ws', 'is_20plus_ws', 'is_40plus_ws', 'is_lose_sb']
        linear_denom_metrics = ['is_superball', 'is_m3', 'is_m3', 'is_m3', 'is_m3', 'is_m3', 'is_m3', 'is_m3', 'is_superball']

        for target_metric_name, num_metric_name, denom_metric_name in zip(linear_target_metrics, linear_num_metrics, linear_denom_metrics):
            result = linearisation(
                          data = metrics_data[['abgroup', 'event_user', num_metric_name, denom_metric_name]]
                        , control_group_name = control_group_name
                        , test_group_name = test_group_name
                        , numerator_metric_name = num_metric_name
                        , denominator_metric_name = denom_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

         # no test metrics 

        linear_target_metrics = ['daily_share_rcoins_1att', 'daily_share_rcoins_superball', 'daily_share_coins_1att']
        linear_num_metrics = ['real_coins_spent_1att', 'real_coins_spent_superball', 'coins_spent_1att']
        linear_denom_metrics = ['real_coins_spent', 'real_coins_spent', 'coins_spent']

        for target_metric_name, num_metric_name, denom_metric_name in zip(linear_target_metrics, linear_num_metrics, linear_denom_metrics):
            result = no_test_ratio(
                          data = metrics_data[['abgroup', 'event_user', num_metric_name, denom_metric_name]]
                        , control_group_name = control_group_name
                        , test_group_name = test_group_name
                        , numerator_metric_name = num_metric_name
                        , denominator_metric_name = denom_metric_name
            )
            result.metric = target_metric_name
            comparison_results.append(result)

    # форматируем итоговый отчет 

    report = pd.DataFrame([r.__dict__ for r in comparison_results])

    report['is_significant'] = report['pvalue'] < significance_level
    report['pvalue'] = report['pvalue'].map('{:.4f}'.format)
    report['left_bound'] = report['left_bound'].map('{:.2%}'.format)
    report['right_bound'] = report['right_bound'].map('{:.2%}'.format)
    report['mean_control'] = report['mean_control'].map('{:.3f}'.format)
    report['mean_test'] = report['mean_test'].map('{:.3f}'.format)
    report['effect'] = report['effect'].map('{:.2%}'.format)

    print(f'Comparing {test_group_name} in reference to {control_group_name} at {significance_level} significance level')
    print(f'Showing {metric_set} metrics for {dates} day {n_day}')

    return report 


__all__ = [
    'compare_metrics'
]
