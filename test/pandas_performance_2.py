import numpy as np
import pandas as pd

# ------------------------------------------------------------------------------

timestamps = pd.date_range("1985-01-01", "2019-08-15", freq='S')
df = pd.DataFrame({"timestamp": timestamps,
                   "normal": np.random.normal(size=len(timestamps)),
                   "log_normal": np.random.lognormal(size=len(timestamps)),
                   "exponential": np.random.exponential(size=len(timestamps)),
                   })
df.set_index("timestamp")

print("All memory allocations are done. Calculating means ...")

m1: pd.Series = df["normal"].ewm(span=3).mean()
m2: pd.Series = df["log_normal"].ewm(span=3).mean()
m3: pd.Series = df["exponential"].ewm(span=3).mean()
print(f"{m1[100]}, {m2[100]}, {m3[100]}")

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:
