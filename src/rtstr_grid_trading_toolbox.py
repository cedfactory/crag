import pandas as pd
import numpy as np
import math
from scipy.stats import chi2, chisquare

# https://www.jmp.com/fr_fr/statistics-knowledge-portal/chi-square-test/chi-square-goodness-of-fit-test.html
# https://www.jmp.com/fr_fr/statistics-knowledge-portal/chi-square-test/chi-square-distribution.html
# https://sites.google.com/view/aide-python/statistiques/test-du-%CF%87-khi2
def test_chi2(df):
    print("Test Chi2")
    # occurences
    n_occurences = df.sum()
    print("total occurences =", n_occurences)

    # choices
    n_choices = df.size
    print("n choices = ", n_choices)

    # expected value for each choice
    expected_occurence_per_choice = n_occurences / n_choices
    print("expected_occurence_per_choice = ", expected_occurence_per_choice)

    # occurences
    array_occurences = df.to_numpy()

    #toto1 = chisquare([16, 18, 16, 14, 12, 12])
    #toto2 = chisquare([180, 250, 120, 225, 225])

    # expected array
    array_expected_occurences = np.full((n_choices), expected_occurence_per_choice)

    res = chisquare(array_occurences)

    # diff squared
    diff_squared = (df - expected_occurence_per_choice) * (df - expected_occurence_per_choice)
    print(diff_squared)

    # test statistic
    test_statistic = diff_squared.sum() / expected_occurence_per_choice
    print("test_statistic = ", test_statistic)

    # risk : 5%
    risk = 5
    significance_threshold = 5 / 100

    # degrees_of_freedom
    degrees_of_freedom = n_choices - 1

    res = chi2(significance_threshold, degrees_of_freedom)
    print(res)


def make_trace(df, df_grid):
    df_trace = df_grid.map(lambda x: 0)
    for i in range(len(df) - 1):
        min = df.iloc[i] if (df.iloc[i] < df.iloc[i + 1]) else df.iloc[i + 1]
        max = df.iloc[i] if (df.iloc[i] > df.iloc[i + 1]) else df.iloc[i + 1]
        # print(min, "->", max)
        df_tmp = df_grid.map(lambda x: 1 if (x >= min and x < max) else 0)
        # print(df_tmp)
        df_trace = df_trace.add(df_tmp)

    # occurences
    occurences = df_trace.sum()
    print("occurences :", occurences)

    # mean
    mean = (df_grid * df_trace).sum() /occurences
    print("mean :", mean)

    # variance
    variance = (df_trace*(df_grid-mean)*(df_grid-mean)).sum()/ occurences
    print("variance :", variance)

    # standard deviation
    std_deviation = math.sqrt(variance)
    print("standard deviation :", std_deviation)

    return df_trace

# variance
# variance = (df_trace*(df_grid-mean)*(df_grid-mean)).sum()/n
# print(df_trace*(df_grid-mean)*(df_grid-mean))
# print("variance =", variance)