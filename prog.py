#!/usr/bin/env python

import time
import glob
import pandas as pd
import dask.dataframe as dd

#vars
col_mean = 'A'
col_group = 'B'

# with thanks to Fahim Sakri
# https://medium.com/pythonhive/python-decorator-to-measure-the-execution-time-of-methods-fa04cb6bb36d
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result

    return timed

@timeit
def pandas_mean_col_by_col(file_pattern, sep, col_mean, col_group):
    dfs = []
    for file in glob.glob(file_pattern):
        dfs.append(pd.read_csv(file, sep=sep))
    df = pd.concat(dfs, axis=0)
    return df.groupby([col_group])[col_mean].mean()

@timeit
def dask_mean_col_by_col(file_pattern, sep, col_mean, col_group):
    df = dd.read_csv(file_pattern, sep=sep)
    result = df.groupby([col_group])[col_mean].mean()
    return result.compute()

print(pandas_mean_col_by_col(
    file_pattern='data/*.tsv',
    sep="\t",
    col_mean=col_mean,
    col_group='file'
))

print(dask_mean_col_by_col(
    file_pattern='data/*.tsv',
    sep="\t",
    col_mean=col_mean,
    col_group=col_group
))
