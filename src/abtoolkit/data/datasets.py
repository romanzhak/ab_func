from ..config import ResearchConfig
from .tools import Source 
from datetime import date, datetime
from textwrap import dedent
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window
from pyspark.sql.types import *
from databricks.sdk.runtime import spark 

ab_test_users = spark.table(Source.AB_USERS.path) 
sessions = spark.table(Source.SESSIONS.path)
ab_test_users_metrics = spark.table(Source.AB_USERS_METRICS.path)
all_events = spark.table(Source.EVENTS.path)
all_levels = spark.table(Source.LEVELS.path)

def calc_attempts(
    cfg: ResearchConfig,
    start_date: date,
    end_date: date,
    users: DataFrame,
) -> None:
    """
    Функция считает данные по попыткам и записывает их в файл
    на вход нужно передать даты, таблицу с пользователями и путь для сохранения
    таблица с пользователями users: event_user, start_feature_user (первый день юзера в тесте с влиянием), abgroup
    """

    # Вычисляем разницу в днях между двумя датами чтобы записывать итеративно по дню
    days_between = (end_date - start_date).days

    windowUserAsc = Window.partitionBy('event_user').orderBy('client_time')
    pre_boosters = (
        col('boosts_aircraft')
        + col('unlimited_boosts_aircraft')
        + col('boosts_bomb_rocket')
        + col('unlimited_boosts_bomb_rocket')
        + col('boosts_lightning')
        + col('unlimited_boosts_lightning')
    )
    in_boosters = (
        col('boosts_hammer')
        + col('boosts_swap')
        + col('boosts_reshuffle')
        + col('boosts_vertical_line')
        + col('boosts_horizontal_line')
    )

    print('Attempts will be save in this file: ', cfg.filename_attempts)

    for i in range(0, days_between + 1):
        date_i = start_date + datetime.timedelta(days=i)
        date_i_str = date_i.strftime('%Y-%m-%d')
        print('Calculating attempts: DAY', i, ' FROM ', days_between, date_i_str)
        levels_filtered = all_levels.filter((F.col('partition_date').between(date_i_str, date_i_str))).withColumnRenamed(
            'user_id', 'event_user'
        )

        df_attempts = (
            users.join(levels_filtered, ['event_user'], 'inner')
            .withColumn('n_day', F.datediff(col('partition_date'), col('start_feature_user')) + 1)
            .withColumn('coins_spent', F.get_json_object('event_payload', '$.coins_spent'))
            .withColumn('coins_spent_additional_moves', F.get_json_object('event_payload', '$.coins_spent_additional_moves'))
            .withColumn('coins_spent_inlevelboosters', F.get_json_object('event_payload', '$.coins_spent_inlevelboosters'))
            .withColumn('coins_spent_startboosters', F.get_json_object('event_payload', '$.coins_spent_startboosters'))
            .withColumn('real_coins_spent', F.get_json_object('event_payload', '$.real_coins_spent'))
            .withColumn(
                'real_coins_spent_additional_moves', F.get_json_object('event_payload', '$.real_coins_spent_additional_moves')
            )
            .withColumn(
                'real_coins_spent_inlevelboosters', F.get_json_object('event_payload', '$.real_coins_spent_inlevelboosters')
            )
            .withColumn('real_coins_spent_startboosters', F.get_json_object('event_payload', '$.real_coins_spent_startboosters'))
            .withColumn('boosts_aircraft', when(F.get_json_object('event_payload', '$.boosts_aircraft') == True, 1).otherwise(0))
            .withColumn(
                'boosts_bomb_rocket', when(F.get_json_object('event_payload', '$.boosts_bomb_rocket') == True, 1).otherwise(0)
            )
            .withColumn(
                'boosts_lightning', when(F.get_json_object('event_payload', '$.boosts_lightning') == True, 1).otherwise(0)
            )
            .withColumn(
                'unlimited_boosts_aircraft',
                when(F.get_json_object('event_payload', '$.unlimited_boosts_aircraft') == True, 1).otherwise(0),
            )
            .withColumn(
                'unlimited_boosts_bomb_rocket',
                when(F.get_json_object('event_payload', '$.unlimited_boosts_bomb_rocket') == True, 1).otherwise(0),
            )
            .withColumn(
                'unlimited_boosts_lightning',
                when(F.get_json_object('event_payload', '$.unlimited_boosts_lightning') == True, 1).otherwise(0),
            )
            .withColumn('boosts_hammer', F.get_json_object('event_payload', '$.boosts_hammer'))
            .withColumn('boosts_swap', F.get_json_object('event_payload', '$.boosts_swap'))
            .withColumn('boosts_reshuffle', F.get_json_object('event_payload', '$.boosts_reshuffle'))
            .withColumn('boosts_vertical_line', F.get_json_object('event_payload', '$.boosts_vertical_line'))
            .withColumn('boosts_horizontal_line', F.get_json_object('event_payload', '$.boosts_horizontal_line'))
            .withColumn('pre_boosters', pre_boosters)
            .withColumn('in_boosters', in_boosters)
            .withColumn('mortas_helmets', F.get_json_object('event_payload', '$.mortas_helmets'))
            .withColumn('is_streak', when(col('mortas_helmets') == 3, 1).otherwise(0))
            .withColumn('influence', when(col('influence') == True, 1).otherwise(0))
            .withColumn('is_win', when(col('reason') == 'completed', 1).otherwise(0))
            .withColumn('is_lose', 1 - col('is_win'))
            .withColumn(
                'is_superball', when(F.get_json_object('event_payload', '$.boosts_super_lightning') == True, 1).otherwise(0)
            )
            .withColumn('coins_spent_1att', F.when(col('attempt') == 1, col('coins_spent')).otherwise(0))
            .withColumn('coins_spent_2plus_att', F.when(col('attempt') > 1, col('coins_spent')).otherwise(0))
            .withColumn('coins_spent_superball', F.when(col('is_superball') == 1, col('coins_spent')).otherwise(0))
            .withColumn('real_coins_spent_1att', F.when(col('attempt') == 1, col('real_coins_spent')).otherwise(0))
            .withColumn('real_coins_spent_2plus_att', F.when(col('attempt') > 1, col('real_coins_spent')).otherwise(0))
            .withColumn('real_coins_spent_superball', F.when(col('is_superball') == 1, col('real_coins_spent')).otherwise(0))
            .withColumn('levels_winstreak', F.get_json_object('event_payload', '$.levels_winstreak'))
            .withColumn('levels_sb_streak', F.get_json_object('event_payload', '$.levels_sb_streak'))
            .select(
                'event_user',
                'abgroup',
                'n_day',
                col('partition_date').alias('date_attempt'),
                'start_feature_user',
                'client_time',
                'level',
                'complexity',
                'attempt',
                'reason',
                'coins_spent',
                'coins_spent_additional_moves',
                'coins_spent_inlevelboosters',
                'coins_spent_startboosters',
                'real_coins_spent',
                'real_coins_spent_additional_moves',
                'real_coins_spent_inlevelboosters',
                'real_coins_spent_startboosters',
                'pre_boosters',
                'in_boosters',
                'mortas_helmets',
                'is_streak',
                'influence',
                'is_win',
                'is_lose',
                'is_superball',
                'coins_spent_1att',
                'coins_spent_2plus_att',
                'coins_spent_superball',
                'real_coins_spent_1att',
                'real_coins_spent_2plus_att',
                'real_coins_spent_superball',
                'levels_winstreak',
                'levels_sb_streak',
            )
            .filter(col('n_day') > 0)
        )
        df_attempts.write.mode('append').saveAsTable(cfg.filename_attempts)
    print('CALCULATING ATTEMPTS FINISHED', cfg.filename_attempts)


def get_cum_metric(user_days_df: DataFrame, metric: str) -> DataFrame:
    """
    Функция добавляет колонку с кумулятивными значениями метрики по дням для каждого игрока
    """

    windowCumUserDay = Window.partitionBy('event_user').orderBy('n_day').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    if metric in [
        'churn_m3',
        'churn_m3_win',
        'churn_m3_lose',
        'is_5plus_ws',
        'is_10plus_ws',
        'is_20plus_ws',
        'is_40plus_ws',
        'is_lose_sb',
        'is_get_sb',
        'is_stucked',
        'is_super_stucked',
        'is_streak',
        'is_superball',
    ]:
        df = user_days_df.withColumn('cum_user_' + metric, F.max(metric).over(windowCumUserDay))
    else:
        df = user_days_df.withColumn('cum_user_' + metric, F.sum(metric).over(windowCumUserDay))
    return df

def calc_m3_metrics(
        cfg: ResearchConfig, 
        start_date: date, 
        end_date: date, 
        users: DataFrame, 
        need_calc_attempts: bool
) -> DataFrame:
    """
    Функция возращает DataFrame с м3 метриками
    на вход нужно передать даты, таблицу с пользователями, флаг нужно ли считать попытки, путь для сохранения
    таблица с пользователями users: event_user, start_feature_user (первый день юзера в тесте с влиянием), abgroup
    """

    if need_calc_attempts:
        calc_attempts(start_date, end_date, users, cfg.filename_attempts)

    # считаем метрики с группировкой по USER, DAY
    windowUserAsc = Window.partitionBy('event_user').orderBy('client_time')

    atts = spark.table(cfg.filename_attempts)
    df_level_user_day_metrics = (
        atts.withColumn('next_att_time', F.lead('client_time').over(windowUserAsc))
        .withColumn('timediff_att', F.datediff(col('next_att_time'), col('client_time')))
        .withColumn(
            'churn_m3',
            when((col('timediff_att') < 7) | (col('date_attempt') > end_date - datetime.timedelta(days=7)), 0).otherwise(1),
        )
        .withColumn('churn_m3_win', when(col('is_win') == 1, col('churn_m3')).otherwise(0))
        .withColumn('churn_m3_lose', when(col('is_win') == 0, col('churn_m3')).otherwise(0))
        .withColumn('prev_levels_winstreak', F.lag('levels_winstreak').over(windowUserAsc))
        .fillna({'prev_levels_winstreak': 0})
        .withColumn('is_5plus_ws', when(col('prev_levels_winstreak') > 4, 1).otherwise(0))
        .withColumn('is_10plus_ws', when(col('prev_levels_winstreak') > 9, 1).otherwise(0))
        .withColumn('is_20plus_ws', when(col('prev_levels_winstreak') > 19, 1).otherwise(0))
        .withColumn('is_40plus_ws', when(col('prev_levels_winstreak') > 39, 1).otherwise(0))
        .withColumn('is_next_sb', F.lead('is_superball').over(windowUserAsc))
        .withColumn('is_lose_sb', when(col('is_superball') == 1, col('is_lose')).otherwise(0))
        .withColumn('is_get_sb', when((col('is_superball') == 0) & (col('is_next_sb') == 1), F.lit(1)).otherwise(0))
        .withColumn('is_stucked', when(col('attempt') > 20, F.lit(1)).otherwise(0))
        .withColumn('is_super_stucked', when(col('attempt') > 40, F.lit(1)).otherwise(0))
        .groupBy('event_user', 'abgroup', 'n_day')
        .agg(
            F.count('*').alias('attempts'),
            F.sum('coins_spent').alias('coins_spent'),
            F.sum('coins_spent_additional_moves').alias('coins_spent_additional_moves'),
            F.sum('coins_spent_inlevelboosters').alias('coins_spent_inlevelboosters'),
            F.sum('coins_spent_startboosters').alias('coins_spent_startboosters'),
            F.sum('real_coins_spent').alias('real_coins_spent'),
            F.sum('real_coins_spent_additional_moves').alias('real_coins_spent_additional_moves'),
            F.sum('real_coins_spent_inlevelboosters').alias('real_coins_spent_inlevelboosters'),
            F.sum('real_coins_spent_startboosters').alias('real_coins_spent_startboosters'),
            F.sum('pre_boosters').alias('pre_boosters'),
            F.sum('in_boosters').alias('in_boosters'),
            F.sum('is_streak').alias('cnt_streak'),
            F.sum('is_win').alias('wins'),
            F.sum('is_superball').alias('cnt_superball'),
            F.sum('coins_spent_1att').alias('coins_spent_1att'),
            F.sum('coins_spent_2plus_att').alias('coins_spent_2plus_att'),
            F.sum('coins_spent_superball').alias('coins_spent_superball'),
            F.sum('real_coins_spent_1att').alias('real_coins_spent_1att'),
            F.sum('real_coins_spent_2plus_att').alias('real_coins_spent_2plus_att'),
            F.sum('real_coins_spent_superball').alias('real_coins_spent_superball'),
            F.sum('is_lose_sb').alias('cnt_lose_sb'),
            F.sum('is_get_sb').alias('cnt_get_sb'),
            F.max('churn_m3').alias('churn_m3'),
            F.max('churn_m3_win').alias('churn_m3_win'),
            F.max('churn_m3_lose').alias('churn_m3_lose'),
            F.max('is_5plus_ws').alias('is_5plus_ws'),
            F.max('is_10plus_ws').alias('is_10plus_ws'),
            F.max('is_20plus_ws').alias('is_20plus_ws'),
            F.max('is_40plus_ws').alias('is_40plus_ws'),
            F.max('is_lose_sb').alias("is_lose_sb"),
            F.max('is_get_sb').alias('is_get_sb'),
            F.max('is_stucked').alias('is_stucked'),
            F.max('is_super_stucked').alias('is_super_stucked'),
            F.max('is_streak').alias('is_streak'),
            F.max('is_superball').alias('is_superball'),
        )
    )

    # делаем кросс таблицу игроков и дней для расчета кумулятивных метрик по каждому игроку (декартово произведение игроков на дни)
    days = df_level_user_day_metrics.select('n_day').distinct()
    user_days = users.crossJoin(days)

    # добавляем в пустую кросстаблицу м3 метрики
    user_days_df = user_days.join(df_level_user_day_metrics, ['n_day', 'event_user', 'abgroup'], 'left').fillna(0)

    # добавляем кумулятивные значения метрик по дням

    # ниже все возможные метрики, можно выбрать желаемые, чтобы лишнего не считать и не сохранять
    # metrics = ['atempts', 'wins', 'coins_spent', 'coins_spent_additional_moves', 'coins_spent_inlevelboosters', 'coins_spent_startboosters', 'real_coins_spent', 'real_coins_spent_additional_moves', 'real_coins_spent_inlevelboosters', 'real_coins_spent_startboosters', 'pre_boosters', 'in_boosters', 'cnt_streak', 'cnt_win', 'cnt_superball', 'coins_spent_1att', 'coins_spent_2plus_att', 'coins_spent_superball', 'real_coins_spent_1att', 'real_coins_spent_2plus_att', 'real_coins_spent_superball', 'cnt_lose_sb', 'cnt_get_sb', 'churn_m3', 'churn_m3_win', 'churn_m3_lose', 'is_5plus_ws', 'is_10plus_ws', 'is_20plus_ws', 'is_40plus_ws', 'is_lose_sb', 'is_get_sb', 'is_stucked', 'is_super_stucked', 'is_streak', 'is_superball']

    metrics = [
        'attempts',
        'wins',
        'coins_spent',
        'real_coins_spent',
        'cnt_streak',
        'cnt_superball',
        'real_coins_spent_1att',
        'real_coins_spent_2plus_att',
        'real_coins_spent_superball',
        'cnt_lose_sb',
        'cnt_get_sb',
        'churn_m3',
        'churn_m3_win',
        'churn_m3_lose',
        'is_stucked',
        'is_super_stucked',
    ]

    for metric in metrics:
        user_days_df = get_cum_metric(user_days_df, metric)

    return user_days_df


def create_dataset(test_config: ResearchConfig, add_m3_metrics=False, need_calc_attempts=False) -> DataFrame:
    """
    Создание датасета для анализа A/B теста, который будет сохранен во временную таблицу

    Parameters:
    test_config (ResearchConfig): Конфигурация для теста.

    Returns:
    DataFrame: Итоговый датасет для анализа.
    """

    # database name to store
    database_name = f'{test_config.dbname}_base_metrics'
    # Получим всю информацию про тест
    ab_users = ab_test_users.filter((F.col('test_id') == test_config.test_id) & (F.col('app_short') == test_config.app_short))

    ab_metrics = ab_test_users_metrics.filter(
        (F.col('test_id') == test_config.test_id) & (F.col('app_short') == test_config.app_short)
    ).withColumn('n_day', col('personal_test_participation_day_num'))

    test_info = ab_users.select(
        'test_id',
        'app_short',
        'test_start_date',
        'test_end_date',
        'enroll_end_date',
        'feature_start_date',
        'feature_end_date',
    ).distinct()

    # Получение необходимых дат
    # в test_config сохраняются автоматически в мета-данные
    start_date_test = test_config.get_meta('test_start_date')
    end_date_test = test_config.get_meta('test_end_date')
    start_date_feature = test_config.get_meta('feature_start_date')
    end_date_enroll = test_config.get_meta('enroll_end_date')

    # Сегментация по платежеспособности
    # Берем сессии за все даты набора игроков, потом заджоним каждого игрока на дату вступления в тест, чтобы взять актуальные характеристики на момент попадания в тест
    df_users = (
        sessions.filter(
            (F.col('partition_date').between(start_date_test, end_date_enroll)) & (F.col('store') == test_config.store)
        )
        .filter(F.col('session_id') != '-1')
        .withColumn('level', F.get_json_object('user_payload', "$['level']").astype('int'))
        .withColumn('skill', F.get_json_object('user_payload', "$['segment.skill']").astype('float'))
        .withColumn('payment', F.get_json_object('user_payload', "$['segment.payment']").astype('float'))
        .withColumn(
            'skill_payment',
            F.when(F.col('level') < 100, '0_early_game')
            .when(F.col('payment') >= 7.912, '4_super_payers')
            .when(F.col('payment') >= 3.575, '3_top_payers')
            .when(F.col('payment') >= 1.120, '2_normal_payers')
            .when((F.col('payment') > 0) & (F.col('skill') >= 0.829), '1_low_payers_skill_3')
            .when((F.col('payment') > 0) & (F.col('skill') >= 0.684), '1_low_payers_skill_2')
            .when(F.col('payment') > 0, '1_low_payers_skill_1')
            .when(F.col('skill') >= 0.846, '0_non_payers_skill_3')
            .when(F.col('skill') >= 0.736, '0_non_payers_skill_2')
            .otherwise('0_non_payers_skill_1'),
        )
        .withColumn(
            'skill_payment_seg',
            F.coalesce(F.get_json_object('user_payload', "$['segment.skillpayment']"), F.col('skill_payment')),
        )
        .withColumn('payer_type', F.get_json_object('user_payload', '$.payer_type'))
        .withColumn('skill', 20 * F.round(F.col('skill') * 100 / 20))  # считаем скилл (фэилрейт) округленный до 20
        .withColumnRenamed('user_id', 'event_user')
        .groupBy('event_user', 'partition_date')
        .agg(
            F.max('payer_type').alias('payer_type'),
            F.max('skill_payment_seg').alias('skill_payment_seg'),
            F.max('skill').alias('skill'),
            F.min('level').alias('level'),
        )
        .select(
            'event_user', 'skill_payment_seg', 'payer_type', 'skill', col('partition_date').alias('test_enroll_date'), 'level'
        )
    )

    # Игроки теста
    df_test_users = (
        ab_users.filter(F.col('good_user') == True)
        # .filter(F.col("is_whale") == False)
        .withColumn('start_feature_user', F.greatest(F.col('test_enroll_date'), F.col('feature_start_date')))
        .select(
            'test_id',
            'app_short',
            'abgroup',
            'event_user',
            'metric_calc_start_date',
            'device_region',
            F.to_date(col('test_enroll_date')).alias('test_enroll_date'),
            'start_feature_user',
        )
        .join(df_users, ['event_user', 'test_enroll_date'], 'left')
        .fillna(0)
    )

    # Чтобы ускорить вычисления, можно сохранить меньший из df на всех нодах через broadcast
    # Т.к. join'ов будет много и во избежание повторных расчетов (на всякий случай) так же используем cache()
    users = F.broadcast(df_test_users.select('event_user', 'abgroup', 'start_feature_user')).persist()

    # Т.к. PySpark использует Lazy Executions, то нужно вызвать count(), чтобы кэширование действительно произошло
    users.count()

    # Метрики монетизации для CUPED
    df_cuped_metrics = (
        ab_metrics.filter(F.col('metric_name').like('prev_%'))
        .groupBy('event_user', F.abs(F.col('personal_test_participation_day_num')).alias('n_day'))
        .agg(F.sum(F.when(F.col('metric_name') == 'prev_revenue_cum', F.col('value'))).alias('revenue_before'))
    )

    # Основные метрики
    df_main_metrics = (
        ab_metrics.join(users, 'event_user')
        # .withColumn('n_day', F.datediff('calendar_day', F.lit(start_date_feature)) + 1)
        # .withColumn('n_day', F.datediff('calendar_day', 'test_enroll_date') + 1)
        .filter(F.col('n_day') > 0)
        .groupBy('event_user', 'n_day')
        .agg(
            F.sum(F.when(F.col('metric_name') == 'revenue', F.col('value'))).alias('revenue'),
            F.sum(F.when(F.col('metric_name') == 'is_payer', F.col('value'))).alias('converted'),
            # метрики ниже в витрине считаются некорректно
            # F.sum(F.when(F.col('metric_name') == 'game_attempts', F.col('value'))).alias('attempts'),
            # F.sum(F.when(F.col('metric_name') == 'game_wins', F.col('value'))).alias('wins'),
            # F.sum(F.when(F.col('metric_name') == 'game_attempts_cum', F.col('value'))).alias('attempts_cum'),
            # F.sum(F.when(F.col('metric_name') == 'game_wins_cum', F.col('value'))).alias('wins_cum'),
            F.sum(F.when(F.col('metric_name') == 'revenue_cum', F.col('value'))).alias('revenue_cum'),
            F.sum(F.when(F.col('metric_name') == 'is_payer_cum', F.col('value'))).alias('converted_cum'),
            F.sum(F.when(F.col('metric_name') == 'active', F.col('value'))).alias('retained'),
            F.sum(F.when(F.col('metric_name') == 'weekly_churn', F.col('value'))).alias('churn'),
        )
    )

    # Итоговый результат
    df_result = (
        df_test_users.join(df_main_metrics, ['event_user'], 'left')
        .join(df_cuped_metrics, ['event_user', 'n_day'], 'left')
        .withColumn('is_payer', F.when(F.col('revenue') > 0, 1).otherwise(0))
        .withColumn(
            'revenue_before_d14',
            F.max('revenue_before').over(
                Window.partitionBy('event_user').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        )
        .select(
            'test_id',
            'app_short',
            'abgroup',
            'event_user',
            'metric_calc_start_date',
            'device_region',
            'skill_payment_seg',
            'payer_type',
            F.col('skill').cast(IntegerType()),
            F.col('level').cast(IntegerType()),
            F.col('is_payer').cast(IntegerType()),
            F.col('n_day').cast(IntegerType()),
            F.col('revenue_before').cast(FloatType()),
            F.col('retained').cast(IntegerType()),
            F.col('churn').cast(IntegerType()),
            F.col('revenue_cum').cast(FloatType()),
            F.col('converted_cum').cast(IntegerType()),
            # F.col('attempts_cum').cast(IntegerType()),
            # F.col('wins_cum').cast(IntegerType()),
            F.col('revenue').cast(FloatType()),
            F.col('converted').cast(IntegerType()),
            F.col('revenue_before_d14').cast(FloatType()),
        )
    )

    if add_m3_metrics:
        m3_metrics = calc_m3_metrics(test_config, start_date_feature, end_date_test, users, need_calc_attempts)
        df_result = df_result.join(m3_metrics, ['event_user', 'n_day', 'abgroup'], 'left')

    df_result.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(database_name)
    test_config.add_meta('abtests_metrics_base', database_name)
    print(
        dedent(
            f"""\
            The dataset was successfully saved in:
                -> {database_name}
            Use cfg.get_meta('abtests_metrics_base')"""
        )
    )

def prepare_base_metrics(cfg: ResearchConfig) -> DataFrame:
    """
    Aggregate “base” A/B-test metrics for every `(abgroup, n_day)` pair.

    The source table is obtained from
    ``spark.table(cfg.get_meta("abtests_metrics_base"))`` and **must already
    contain per-user aggregates** produced before

    Parameters
    ----------
    cfg :
        Active :class:`~abtoolkit.config.ResearchConfig`.
        In ``cfg.meta`` it must hold the key ``"abtests_metrics_base"`` with
        the fully-qualified Spark table name.

    Returns
    -------
    DataFrame
        ==============  ================================================================
        Column          Definition
        --------------  ------------------------------------------------
        total_users     ``countDistinct(event_user)``
        retained_users  ``sum(retained)``
        churned_users   ``sum(churn)``
        converted_users ``sum(converted_cum)``
        paying_users    ``countDistinct(when(revenue>0, event_user))``
        revenue_cum     ``sum(revenue_cum)``
        revenue         ``sum(revenue)``
        arpu            ``revenue_cum / total_users``
        arppu           ``revenue_cum / converted_users``
        darpu           ``revenue      / retained_users``
        darppu          ``revenue      / paying_users``
        conversion      ``converted_users / total_users * 100`` (%)
        paying_share    ``paying_users  / retained_users * 100`` (%)
        retention       ``retained_users / total_users * 100`` (%)
        churn           ``churned_users / total_users * 100`` (%)
        ==============  ================================================================

    """
    metrics_data = spark.table(cfg.get_meta('abtests_metrics_base'))
    df_base = (
        metrics_data.groupby('abgroup', 'n_day')
        .agg(
            F.countDistinct('event_user').alias('total_users'),
            F.sum('retained').alias('retained_users'),
            F.sum('churn').alias('churned_users'),
            F.sum('converted_cum').alias('converted_users'),
            F.countDistinct(F.when(F.col('revenue') > 0, F.col('event_user'))).alias('paying_users'),
            F.sum('revenue_cum').alias('revenue_cum'),
            F.sum('revenue').alias('revenue'),
            # F.mean('attempts_cum').alias('attempts_mean'),
            # F.mean('attempts').alias('attempts'),
            # F.mean('wins_cum').alias('wins_mean'),
            # F.mean('wins').alias('wins'),
            # F.sum('attempts').alias('sum_attempts'),
            # F.sum('wins').alias('sum_wins'),
            # F.mean('cum_user_attempts').alias('cum_attempts'),
            # F.mean('cum_user_wins').alias('cum_wins')
        )
        .withColumn('arpu', F.col('revenue_cum') / F.col('total_users'))
        .withColumn('arppu', F.col('revenue_cum') / F.col('converted_users'))
        # .withColumn('dwins', F.col('sum_wins') / F.col('retained_users'))
        # .withColumn('dattempts', F.col('sum_attempts') / F.col('retained_users'))
        .withColumn('darpu', F.col('revenue') / F.col('retained_users'))
        .withColumn('darppu', F.col('revenue') / F.col('paying_users'))
        .withColumn('conversion', F.round(F.col('converted_users') / F.col('total_users') * 100, 2))
        .withColumn('paying_share', F.round(F.col('paying_users') / F.col('retained_users') * 100, 2))
        .withColumn('retention', F.round(F.col('retained_users') / F.col('total_users') * 100, 2))
        .withColumn('churn', F.round(F.col('churned_users') / F.col('total_users') * 100, 2))
    )
    return df_base

def prepare_m3_metrics(cfg: ResearchConfig) -> DataFrame:
    """
    Build **“M3” game-core metrics** aggregated by A/B group and day.

    The input table is read from the path stored under
    ``cfg.get_meta("abtests_metrics_base")`` and must already contain per-user
    daily columns listed below.
    Output has one row per ``(abgroup, n_day)`` and the following columns
    (metrics derived with a *daily* prefix are computed per retained user).

    Parameters
    ----------
    cfg :
        Active :class:`~abtoolkit.config.ResearchConfig`.  Must have
        ``meta['abtests_metrics_base']`` pointing to the source Spark table.

    Returns
    -------
    DataFrame
        ========= ================================================================
        Column      Definition
        ---------   -------------------------------------------------------------
        total_users ``countDistinct(event_user)``
        retained_users ``sum(retained)``
        sum_attempts ``sum(attempts)``
        sum_wins   ``sum(wins)``
        cum_attempts   ``mean(cum_user_attempts)``
        cum_wins       ``mean(cum_user_wins)``
        cum_coins_spent            ``mean(cum_user_coins_spent)``
        cum_real_coins_spent       ``mean(cum_user_real_coins_spent)``
        cum_real_coins_spent_1att  ``mean(cum_user_real_coins_spent_1att)``
        cum_real_coins_spent_2plus_att ``mean(cum_user_real_coins_spent_2plus_att)``
        cum_real_coins_spent_superball ``mean(cum_user_real_coins_spent_superball)``
        sum_churn_m3          ``sum(churn_m3)``
        sum_churn_m3_lose     ``sum(churn_m3_lose)``
        sum_churn_m3_win      ``sum(churn_m3_win)``
        is_stucked            ``sum(is_stucked)``
        is_super_stucked      ``sum(is_super_stucked)``
        coins_spent           ``sum(coins_spent)``
        real_coins_spent      ``sum(real_coins_spent)``
        real_coins_spent_1att ``sum(real_coins_spent_1att)``
        coins_spent_1att      ``sum(coins_spent_1att)``
        real_coins_spent_2plus_att ``sum(real_coins_spent_2plus_att)``
        dwins                ``sum_wins / retained_users``
        dattempts            ``sum_attempts / retained_users``
        daily_share_stucked          ``is_stucked / retained_users * 100`` (%)
        daily_share_super_stucked    ``is_super_stucked / retained_users * 100`` (%)
        daily_coins_spent            ``coins_spent / retained_users``
        daily_real_coins_spent       ``real_coins_spent / retained_users``
        daily_real_coins_spent_1att  ``real_coins_spent_1att / retained_users``
        daily_real_coins_spent_2plus_att ``real_coins_spent_2plus_att / retained_users``
        daily_churn_m3        ``sum_churn_m3 / retained_users * 100`` (%)
        daily_churn_m3_lose   ``sum_churn_m3_lose / retained_users * 100`` (%)
        daily_churn_m3_win    ``sum_churn_m3_win / retained_users * 100`` (%)
        daily_share_rcoins_1att ``real_coins_spent_1att / real_coins_spent * 100`` (%)
        daily_share_coins_1att  ``coins_spent_1att / coins_spent * 100`` (%)
        cum_share_rcoins_1att   ``cum_real_coins_spent_1att / cum_real_coins_spent * 100`` (%)
        ========= ================================================================
    """
    metrics_data = spark.table(cfg.get_meta('abtests_metrics_base'))
    df_m3 = (
        metrics_data.groupby('abgroup', 'n_day')
        .agg(
            F.countDistinct('event_user').alias('total_users'),
            F.sum('retained').alias('retained_users'),
            F.sum('attempts').alias('sum_attempts'),
            F.sum('wins').alias('sum_wins'),
            F.mean('cum_user_attempts').alias('cum_attempts'),
            F.mean('cum_user_wins').alias('cum_wins'),
            F.mean('cum_user_coins_spent').alias('cum_coins_spent'),
            F.mean('cum_user_real_coins_spent').alias('cum_real_coins_spent'),
            F.mean('cum_user_real_coins_spent_1att').alias('cum_real_coins_spent_1att'),
            # F.mean('cum_user_coins_spent_1att').alias('cum_coins_spent_1att'),
            F.mean('cum_user_real_coins_spent_2plus_att').alias('cum_real_coins_spent_2plus_att'),
            F.mean('cum_user_real_coins_spent_superball').alias('cum_real_coins_spent_superball'),
            # F.mean('cum_user_churn_m3').alias('cum_churn_m3'),
            # F.mean('cum_user_churn_m3_win').alias('cum_churn_m3_win'),
            # F.mean('cum_user_churn_m3_lose').alias('cum_churn_m3_lose'),
            # F.mean('cum_user_is_stucked').alias('cum_is_stucked'),
            # F.mean('cum_user_is_super_stucked').alias('cum_is_super_stucked'),
            F.sum('churn_m3').alias('sum_churn_m3'),
            F.sum('churn_m3_lose').alias('sum_churn_m3_lose'),
            F.sum('churn_m3_win').alias('sum_churn_m3_win'),
            F.sum('is_stucked').alias('is_stucked'),
            F.sum('is_super_stucked').alias('is_super_stucked'),
            F.sum('coins_spent').alias('coins_spent'),
            F.sum('real_coins_spent').alias('real_coins_spent'),
            F.sum('real_coins_spent_1att').alias('real_coins_spent_1att'),
            F.sum('coins_spent_1att').alias('coins_spent_1att'),
            F.sum('real_coins_spent_2plus_att').alias('real_coins_spent_2plus_att'),
        )
        .withColumn('dwins', F.col('sum_wins') / F.col('retained_users'))
        .withColumn('dattempts', F.col('sum_attempts') / F.col('retained_users'))
        .withColumn('daily_share_stucked', 100 * F.col('is_stucked') / F.col('retained_users'))
        .withColumn('daily_share_super_stucked', 100 * F.col('is_super_stucked') / F.col('retained_users'))
        .withColumn('daily_coins_spent', F.col('coins_spent') / F.col('retained_users'))
        .withColumn('daily_real_coins_spent', F.col('real_coins_spent') / F.col('retained_users'))
        .withColumn('daily_real_coins_spent_1att', F.col('real_coins_spent_1att') / F.col('retained_users'))
        .withColumn('daily_real_coins_spent_2plus_att', F.col('real_coins_spent_2plus_att') / F.col('retained_users'))
        .withColumn('daily_churn_m3', 100 * F.col('sum_churn_m3') / F.col('retained_users'))
        .withColumn('daily_churn_m3_lose', 100 * F.col('sum_churn_m3_lose') / F.col('retained_users'))
        .withColumn('daily_churn_m3_win', 100 * F.col('sum_churn_m3_win') / F.col('retained_users'))
        .withColumn('daily_share_rcoins_1att', 100 * F.col('real_coins_spent_1att') / F.col('real_coins_spent'))
        .withColumn('daily_share_coins_1att', 100 * F.col('coins_spent_1att') / F.col('coins_spent'))
        .withColumn('cum_share_rcoins_1att', 100 * F.col('cum_real_coins_spent_1att') / F.col('cum_real_coins_spent'))
        # .withColumn('cum_share_coins_1att', F.col('cum_coins_spent_1att') / F.col('cum_coins_spent'))
    )
    return df_m3

def prepare_streak_metrics(cfg: ResearchConfig) -> DataFrame:
    """
    Build **“streak / super-ball” engagement metrics** per A/B group and day.

    The source table is read from the path stored in
    ``cfg.get_meta("abtests_metrics_base")`` and must contain the daily,
    per-user columns referenced below.  The function returns one aggregated
    row per ``(abgroup, n_day)`` with the metrics listed later.

    Parameters
    ----------
    cfg :
        Active :class:`~abtoolkit.config.ResearchConfig` instance.
        Must contain the key ``"abtests_metrics_base"`` in ``cfg.meta``.

    Returns
    -------
    DataFrame
        ========= ================================================================
        Column     Definition
        ---------  ---------------------------------------------------------------
        total_users               ``countDistinct(event_user)``
        m3_users                  ``sum(is_m3)``
        sb_users                  ``sum(is_superball)``
        streak_users              ``sum(is_streak)``
        lose_sb_users             ``sum(is_lose_sb)``
        not_sb_not_streak_users   ``sum(is_not_sb_not_streak)``
        sum_attempts              ``sum(attempts)``
        coins_spent               ``sum(coins_spent)``
        real_coins_spent          ``sum(real_coins_spent)``
        real_coins_spent_1att     ``sum(real_coins_spent_1att)``
        real_coins_spent_superball ``sum(real_coins_spent_superball)``
        coins_spent_1att          ``sum(coins_spent_1att)``
        real_coins_spent_2plus_att ``sum(real_coins_spent_2plus_att)``
        cum_cnt_superball         ``mean(cum_user_cnt_superball)``
        cum_cnt_lose_sb           ``mean(cum_user_cnt_lose_sb)``
        daily_avg_length_sb_streak ``sum(daily_avg_length_sb_streak) / sb_users``
        is_5plus_ws … is_40plus_ws  cumulative counts of win-streaks ≥ 5/10/20/40
        share_sb_users            ``sb_users / m3_users * 100`` (%)
        share_lost_sb_users       ``lose_sb_users / sb_users * 100`` (%)
        share_not_sb_not_streak   ``not_sb_not_streak_users / m3_users * 100`` (%)
        share_streak_users        ``streak_users / m3_users * 100`` (%)
        daily_share_users_5plus_ws   ``is_5plus_ws / m3_users * 100`` (%)
        daily_share_users_10plus_ws  ``is_10plus_ws / m3_users * 100`` (%)
        daily_share_users_20plus_ws  ``is_20plus_ws / m3_users * 100`` (%)
        daily_share_users_40plus_ws  ``is_40plus_ws / m3_users * 100`` (%)
        daily_share_rcoins_1att      ``real_coins_spent_1att / real_coins_spent * 100`` (%)
        daily_share_rcoins_superball ``real_coins_spent_superball / real_coins_spent * 100`` (%)
        daily_share_coins_1att       ``coins_spent_1att / coins_spent * 100`` (%)
        ========= ================================================================
    """
    metrics_data = spark.table(cfg.get_meta('abtests_metrics_base'))
    df_streak = (
        metrics_data.withColumn('is_m3', when(col('attempts') > 0, 1).otherwise(0))
        .withColumn(
            'is_not_sb_not_streak',
            when((col('is_superball') == 0) & (col('is_streak') == 0) & (col('is_m3') == 1), 1).otherwise(0),
        )
        # .withColumn('cum_delimeter_for_len_streak', when((col('cum_cnt_superball')>0) & (col('cum_cnt_lose_sb')==0), 1).otherwise(col('cum_cnt_lose_sb')))
        # .withColumn('cum_avg_length_sb_streak', F.col('cum_cnt_superball')/F.col('delimeter_for_len_streak'))
        .withColumn(
            'daily_delimeter_for_len_streak',
            when((col('cnt_superball') > 0) & (col('cnt_lose_sb') == 0), 1).otherwise(col('cnt_lose_sb')),
        )
        .withColumn('daily_avg_length_sb_streak', F.col('cnt_superball') / F.col('daily_delimeter_for_len_streak'))
        .groupby('abgroup', 'n_day')
        .agg(
            F.countDistinct('event_user').alias('total_users'),
            F.sum('is_m3').alias('m3_users'),
            F.sum('is_superball').alias('sb_users'),
            F.sum('is_streak').alias('streak_users'),
            F.sum('is_lose_sb').alias('lose_sb_users'),
            F.sum('is_not_sb_not_streak').alias('not_sb_not_streak_users'),
            F.sum('attempts').alias('sum_attempts'),
            F.sum('coins_spent').alias('coins_spent'),
            F.sum('real_coins_spent').alias('real_coins_spent'),
            F.sum('real_coins_spent_1att').alias('real_coins_spent_1att'),
            F.sum('real_coins_spent_superball').alias('real_coins_spent_superball'),
            F.sum('coins_spent_1att').alias('coins_spent_1att'),
            F.sum('real_coins_spent_2plus_att').alias('real_coins_spent_2plus_att'),
            F.mean('cum_user_cnt_superball').alias('cum_cnt_superball'),
            F.mean('cum_user_cnt_lose_sb').alias('cum_cnt_lose_sb'),
            F.mean('cum_user_cnt_superball').alias('cum_cnt_superball'),
            F.mean('cum_user_cnt_lose_sb').alias('cum_cnt_lose_sb'),
            # F.sum('cum_avg_length_sb_streak').alias('cum_avg_length_sb_streak'),
            F.sum('daily_avg_length_sb_streak').alias('daily_avg_length_sb_streak'),
            F.sum('is_5plus_ws').alias('is_5plus_ws'),
            F.sum('is_10plus_ws').alias('is_10plus_ws'),
            F.sum('is_20plus_ws').alias('is_20plus_ws'),
            F.sum('is_40plus_ws').alias('is_40plus_ws'),
        )
        .withColumn('share_sb_users', 100 * F.col('sb_users') / F.col('m3_users'))
        .withColumn('share_lost_sb_users', 100 * F.col('lose_sb_users') / F.col('sb_users'))
        .withColumn('share_not_sb_not_streak', 100 * F.col('not_sb_not_streak_users') / F.col('m3_users'))
        .withColumn('share_streak_users', 100 * F.col('streak_users') / F.col('m3_users'))
        .withColumn('daily_avg_length_sb_streak', F.col('daily_avg_length_sb_streak') / F.col('sb_users'))
        .withColumn('daily_share_users_5plus_ws', 100 * F.col('is_5plus_ws') / F.col('m3_users'))
        .withColumn('daily_share_users_10plus_ws', 100 * F.col('is_10plus_ws') / F.col('m3_users'))
        .withColumn('daily_share_users_20plus_ws', 100 * F.col('is_20plus_ws') / F.col('m3_users'))
        .withColumn('daily_share_users_40plus_ws', 100 * F.col('is_40plus_ws') / F.col('m3_users'))
        .withColumn('daily_share_rcoins_1att', 100 * F.col('real_coins_spent_1att') / F.col('real_coins_spent'))
        .withColumn('daily_share_rcoins_superball', 100 * F.col('real_coins_spent_superball') / F.col('real_coins_spent'))
        .withColumn('daily_share_coins_1att', 100 * F.col('coins_spent_1att') / F.col('coins_spent'))
    )
    return df_streak

__all__ = [
    'create_dataset',
    'prepare_base_metrics',
    'prepare_m3_metrics',
    'prepare_streak_metrics',
]
