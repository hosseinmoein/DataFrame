import numpy as np
import pandas as pd
import time

# ------------------------------------------------------------------------------

start: float = float(time.time())

length: int = 100000000

df = pd.DataFrame(
         { "INDEX": np.arange(0, length),
           "uniform": np.random.randint(-10000000, high=10000000, size=length, dtype=np.int64)
         })
df.set_index("INDEX")

middle: float = float(time.time())
result = (df["uniform"] - 2).abs().sum()

last: float = float(time.time())

print(f"Result: {result}")
print(f"Calculation Time: {last - middle}\nOverall Time: {last - start}")

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:
