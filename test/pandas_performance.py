import time
import numpy as np
import pandas as pd

# ------------------------------------------------------------------------------

print(f"Starting ... {int(time.time())}");

timestamps = pd.date_range("1970-01-01", "2019-08-15", freq='S')
df = pd.DataFrame({"timestamp": timestamps,
                   "normal": np.random.normal(size=len(timestamps)),
                   "log_normal": np.random.lognormal(size=len(timestamps)),
                   "exponential": np.random.exponential(size=len(timestamps)),
                   })
df.set_index("timestamp")

print(f"All memory allocations are done. Calculating means ... {int(time.time())}")

m1: float = df["normal"].mean()
m2: float = df["log_normal"].mean()
m3: float = df["exponential"].mean()
print(f"{m1}, {m2}, {m3}")
print(f"{int(time.time())} ... Done");

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:

