import datetime
import numpy as np
import polars as pl

# ------------------------------------------------------------------------------

SIZE: int = 100000000

first = datetime.datetime.now()
df = pl.DataFrame({"normal": np.random.normal(size=SIZE),
                   "log_normal": np.random.lognormal(size=SIZE),
                   "exponential": np.random.exponential(size=SIZE),
                   })
second = datetime.datetime.now()
print(f"All data loadings are done. Calculating means ... "
      f"{(second - first).seconds}.{(second - first).microseconds}")

m1: float = df["normal"].mean()
m2: float = df["log_normal"].var()
m3: float = df.select(pl.corr("exponential", "log_normal"))

print(f"{m1}, {m2}, {m3}")
third = datetime.datetime.now()


print(f"{(third - second).seconds}.{(third - second).microseconds}, "
      f"{(third - first).seconds}.{(third - first).microseconds} All done");

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:
