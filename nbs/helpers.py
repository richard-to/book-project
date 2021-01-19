import matplotlib.pyplot as plt

import pyspark.sql.functions as F
from pyspark_dist_explore import hist


def get_basic_counts(df, col):
    df.agg(
        F.count(col),
        F.countDistinct(col),
    ).show()


def check_nulls(df, null_col, id_coll):
    df.where(null_col.isNull()).agg(
        F.count(id_coll).alias(f"Has Null ({null_col._jc.toString()})"),
    ).show()


def check_empty_strings(df, col):
    df.where(F.trim(col) == "").agg(
        F.count(col).alias(f"Has Empty ({col._jc.toString()})"),
    ).show()


def check_lengths(df, col):
    (
        df
        .select(col, F.length(col))
        .sort(F.length(col).asc())
        .show(10)
    )
    (
        df
        .select(col, F.length(col))
        .sort(F.length(col).desc())
        .show(10)
    )


def basic_stats(df, col):
    df.agg(
        F.count(col),
        F.avg(col),
        F.min(col),
        F.max(col),
    ).show()


def plot_hist(df, bins, title=None):
    fig, ax = plt.subplots()
    hist(ax, df, bins=bins, color=["blue"])
    if title:
        ax.set_title(title)
        _ = ax.legend()
