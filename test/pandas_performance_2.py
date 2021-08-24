import numpy as np
import pandas as pd
import time

# ------------------------------------------------------------------------------

print(f"Starting ... {int(time.time())}");

length: int = 1100000000

df = pd.DataFrame({"INDEX": np.arange(0, length),
                   "normal": np.random.normal(size=length),
                   "log_normal": np.random.lognormal(size=length),
                   "exponential": np.random.exponential(size=length),
                   })
df.set_index("INDEX")

print(f"All memory allocations are done. Calculating means ... {int(time.time())}")

m1: pd.Series = df["normal"].ewm(span=3).mean()
m2: pd.Series = df["log_normal"].ewm(span=3).mean()
m3: pd.Series = df["exponential"].ewm(span=3).mean()

print(f"{m1[100000]}, {m2[100000]}, {m3[100000]}")
print(f"{int(time.time())} ... Done");

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:
