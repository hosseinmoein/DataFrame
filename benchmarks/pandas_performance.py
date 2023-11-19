import datetime
import numpy as np
import pandas as pd

# ------------------------------------------------------------------------------

SIZE: int = 300000000

first = datetime.datetime.now()
df = pd.DataFrame({"normal": np.random.normal(size=SIZE),
                   "log_normal": np.random.lognormal(size=SIZE),
                   "exponential": np.random.exponential(size=SIZE),
                   })
second = datetime.datetime.now()
print(f"Data generation/load time: "
      f"{(second - first).seconds}.{(second - first).microseconds}")

mean: float = df["normal"].mean()
var: float = df["log_normal"].var()
corr: float = df["exponential"].corr(df["log_normal"])

print(f"{mean}, {var}, {corr}")
third = datetime.datetime.now()

df2 = df[df["log_normal"] > 8]
print(f"Number of rows after select: {len(df2)}")
fourth = datetime.datetime.now()

print(f"Calculation time: {(third - second).seconds}.{(third - second).microseconds}")
print(f"Selection time: {(fourth - third).seconds}.{(fourth - third).microseconds}")
print(f"Overall time: {(fourth - first).seconds}.{(fourth - first).microseconds}")

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:

