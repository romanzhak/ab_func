from dataclasses import dataclass
import pandas as pd
from pandas import DataFrame
import numpy as np
import scipy.stats as sps

@dataclass
class ExperimentComparisonResults:
    metric: str
    mean_control: float
    mean_test: float
    effect: float
    pvalue: float
    left_bound: float
    right_bound: float
    stat_test: str

def relative_ttest(data: DataFrame, control_group_name: str, test_group_name: str, metric_name: str):
    """
    Parameters
    ----------
    data : DataFrame with columns 'abgroup', 'event_user', {metric_name}
    control_group_name : name of the referential group in the experiment
    test_group_name : name of the group to compare
    metric_name : name of the target metric column

    Returns
    -------
    ExperimentComparisonResults dict 
        ==============  ================================================================
        Column          Definition
        --------------  ------------------------------------------------
        metric_name     the metric to compare
        mean_control    mean of the control group
        mean_test       mean of the test group
        effect          relative effect size (test to control)
        pvalue          p-value
        left_bound      lower bound of the relative confidence interval
        right_bound     upper bound of the relative confidence interval
        ==============  ================================================================
    """

    control = data.loc[data['abgroup'] == control_group_name, metric_name].to_numpy()
    test = data.loc[data['abgroup'] == test_group_name, metric_name].to_numpy()

    mean_control = np.mean(control)
    mean_test = np.mean(test)
    var_mean_control  = np.var(control) / len(control)
    var_mean_test  = np.var(test) / len(test)

    if mean_control == 0.0:
        effect = 0
        pvalue, left_bound, right_bound = np.nan, np.nan, np.nan
    else:
        difference_mean = mean_test - mean_control
        difference_mean_var  = var_mean_test + var_mean_control
        
        covariance = -var_mean_control

        relative_mu = difference_mean / mean_control
        relative_var = difference_mean_var / (mean_control ** 2) \
                        + var_mean_control * ((difference_mean ** 2) / (mean_control ** 4))\
                        - 2 * (difference_mean / (mean_control ** 3)) * covariance
        relative_distribution = sps.norm(loc=relative_mu, scale=np.sqrt(relative_var))
        left_bound, right_bound = relative_distribution.ppf([0.025, 0.975])
        
        pvalue = 2 * min(relative_distribution.cdf(0), relative_distribution.sf(0))
        effect = relative_mu

    stat_test = 'relative ttest'

    return ExperimentComparisonResults(metric_name, mean_control, mean_test, effect, pvalue, left_bound, right_bound, stat_test)

def relative_cuped(data: DataFrame, control_group_name: str, test_group_name: str, metric_name: str, metric_before_name: str):
    """
    Parameters
    ----------
    data : DataFrame with columns 'abgroup', 'event_user', {metric_name}, {metric_before_name}
    control_group_name : name of the referential group in the experiment
    test_group_name : name of the group to compare
    metric_name : name of the target metric column
    metric_before_name : name of the pre-test metric column

    Returns
    -------
    ExperimentComparisonResults dict 
        ==============  ================================================================
        Column          Definition
        --------------  ------------------------------------------------
        metric_name     the metric to compare
        mean_control    mean of the control group
        mean_test       mean of the test group
        effect          relative effect size (test to control)
        pvalue          p-value
        left_bound      lower bound of the relative confidence interval
        right_bound     upper bound of the relative confidence interval
        ==============  ================================================================
    """
    control = data.loc[data['abgroup'] == control_group_name, metric_name].to_numpy()
    control_before = data.loc[data['abgroup'] == control_group_name, metric_before_name].to_numpy()
    test = data.loc[data['abgroup'] == test_group_name, metric_name].to_numpy()
    test_before = data.loc[data['abgroup'] == test_group_name, metric_before_name].to_numpy()

    theta = (np.cov(control, control_before)[0, 1] + np.cov(test, test_before)[0, 1]) /\
                (np.var(control_before) + np.var(test_before))

    control_cup = control - theta * control_before
    test_cup = test - theta * test_before

    mean_den = np.mean(control)
    mean_num = np.mean(test_cup) - np.mean(control_cup)
    var_mean_den  = np.var(control) / len(control)
    var_mean_num  = np.var(test_cup) / len(test_cup) + np.var(control_cup) / len(control_cup)

    cov = -np.cov(control_cup, control)[0, 1] / len(control)

    relative_mu = mean_num / mean_den
    relative_var = var_mean_num / (mean_den ** 2)  + var_mean_den * ((mean_num ** 2) / (mean_den ** 4))\
                - 2 * (mean_num / (mean_den ** 3)) * cov
    
    relative_distribution = sps.norm(loc=relative_mu, scale=np.sqrt(relative_var))
    left_bound, right_bound = relative_distribution.ppf([0.025, 0.975])
    
    pvalue = 2 * min(relative_distribution.cdf(0), relative_distribution.sf(0))
    effect = relative_mu
    mean_control = np.mean(control)
    mean_test = np.mean(test)
    stat_test = 'relative cuped'

    return ExperimentComparisonResults(metric_name, mean_control, mean_test, effect, pvalue, left_bound, right_bound, stat_test)
  
def linearisation(data: DataFrame, control_group_name: str, test_group_name: str, numerator_metric_name: str, denominator_metric_name: str):
    """
    Parameters
    ----------
    data : DataFrame with columns 'abgroup', 'event_user', {numerator_metric_name}, {denominator_metric_name}
    control_group_name : name of the referential group in the experiment
    test_group_name : name of the group to compare
    numerator_metric_name : name of the numerator column of the ratio-metric 
    denominator_metric_name : name of the denominator column of the ratio-metric 

    Returns
    -------
    ExperimentComparisonResults dict 
        ==============  ================================================================
        Column          Definition
        --------------  ------------------------------------------------
        metric_name     the metric to compare
        mean_control    mean of the control group
        mean_test       mean of the test group
        effect          relative effect size (test to control)
        pvalue          p-value
        left_bound      lower bound of the relative confidence interval
        right_bound     upper bound of the relative confidence interval
        ==============  ================================================================
    """
    
    num_control = data.loc[data['abgroup'] == control_group_name, numerator_metric_name].sum()
    num_test = data.loc[data['abgroup'] == test_group_name, numerator_metric_name].sum()
    denom_control = data.loc[data['abgroup'] == control_group_name, denominator_metric_name].sum()
    denom_test = data.loc[data['abgroup'] == test_group_name, denominator_metric_name].sum()

    if denom_control == 0 or denom_test == 0 or num_control == 0:
        mean_control_original, mean_test_original = np.nan, np.nan
        relative_effect, pvalue, relative_left_b, relative_right_b = np.nan, np.nan, np.nan, np.nan

    else:
        mean_control_original = num_control / denom_control
        mean_test_original = num_test / denom_test

        # вводим линеаризованную метрику 
        data = data.copy()
        data['linear_metric'] = data[numerator_metric_name] - mean_control_original * data[denominator_metric_name]

        # проводим для нее абсолютный ttest 
        control = data.loc[data['abgroup'] == control_group_name, 'linear_metric'].to_numpy()
        test = data.loc[data['abgroup'] == test_group_name, 'linear_metric'].to_numpy()

        mean_control = np.mean(control)
        mean_test = np.mean(test)
        var_mean_control  = np.var(control) / len(control)
        var_mean_test  = np.var(test) / len(test)
        
        difference_mean = mean_test - mean_control
        difference_mean_var = var_mean_control + var_mean_test
        difference_distribution = sps.norm(loc=difference_mean, scale=np.sqrt(difference_mean_var))

        left_bound, right_bound = difference_distribution.ppf([0.025, 0.975])
        pvalue = 2 * min(difference_distribution.cdf(0), difference_distribution.sf(0))
        effect = difference_mean

        # переходим обратно к исходной метрике и строим относительный интервал для отчета 
        relative_effect = effect / mean_control_original
        relative_left_b = left_bound / mean_control_original
        relative_right_b = right_bound / mean_control_original

    metric_name = 'linearised'
    stat_test = 'linearisation'

    return ExperimentComparisonResults(metric_name, mean_control_original, mean_test_original, relative_effect, pvalue, relative_left_b, relative_right_b, stat_test)

def no_test_ratio(data: DataFrame, control_group_name: str, test_group_name: str, numerator_metric_name: str, denominator_metric_name: str):
    """
    Функция используется для вычисления базового сравнения по ratio метрикам, когда стандартные стат. тесты не работают.
    """


    mean_control = (data.loc[data['abgroup'] == control_group_name, numerator_metric_name].sum() \
      / data.loc[data['abgroup'] == control_group_name, denominator_metric_name].sum())
    mean_test = (data.loc[data['abgroup'] == test_group_name, numerator_metric_name].sum() \
      / data.loc[data['abgroup'] == test_group_name, denominator_metric_name].sum())

    difference_mean = mean_test - mean_control
    effect = difference_mean / mean_control
    stat_test = 'no test'
    metric_name = 'linearised'

    return ExperimentComparisonResults(metric_name, mean_control, mean_test, effect, np.nan, np.nan, np.nan, stat_test)


__all__ = [
    'relative_ttest',
    'relative_cuped',
    'linearisation',
    'no_test_ratio'
]
