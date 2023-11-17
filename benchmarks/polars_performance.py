import datetime
import numpy as np
import polars as pl

# ------------------------------------------------------------------------------

SIZE: int = 200000000

first = datetime.datetime.now()
df = pl.DataFrame({"normal": np.random.normal(size=SIZE),
                   "log_normal": np.random.lognormal(size=SIZE),
                   "exponential": np.random.exponential(size=SIZE),
                   })
second = datetime.datetime.now()
print(f"Data generation/load time: "
      f"{(second - first).seconds}.{(second - first).microseconds}")

df = df.select(
   mean = pl.col("normal").mean(),
   var = pl.col("log_normal").var(),
   corr = pl.corr("exponential", "log_normal")
)

mean: float = df["mean"]
var: float = df["var"]
corr: float = df["corr"]

print(f"{mean[0]}, {var[0]}, {corr[0]}")
third = datetime.datetime.now()

print(f"Calculation time: {(third - second).seconds}.{(third - second).microseconds}")
print(f"Overall time: {(third - first).seconds}.{(third - first).microseconds} All done");

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:
