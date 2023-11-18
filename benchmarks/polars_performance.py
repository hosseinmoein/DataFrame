import datetime
import numpy as np
import polars as pl

# ------------------------------------------------------------------------------

SIZE: int = 300000000

first = datetime.datetime.now()
df = pl.DataFrame({"normal": np.random.normal(size=SIZE),
                   "log_normal": np.random.lognormal(size=SIZE),
                   "exponential": np.random.exponential(size=SIZE),
                   })
second = datetime.datetime.now()
print(f"Data generation/load time: "
      f"{(second - first).seconds}.{(second - first).microseconds}")

df2 = df.select(
   mean = pl.col("normal").mean(),
   var = pl.col("log_normal").var(),
   corr = pl.corr("exponential", "log_normal")
)

mean: float = df2["mean"]
var: float = df2["var"]
corr: float = df2["corr"]

print(f"{mean[0]}, {var[0]}, {corr[0]}")
third = datetime.datetime.now()

df3 = df.filter(pl.col("log_normal") > 8)
print(f"Number of rows after select: {df3.select(pl.count()).item()}")
fourth = datetime.datetime.now()

print(f"Calculation time: {(third - second).seconds}.{(third - second).microseconds}")
print(f"Selection time: {(fourth - third).seconds}.{(third - second).microseconds}")
print(f"Overall time: {(fourth - first).seconds}.{(fourth - first).microseconds}")

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:
