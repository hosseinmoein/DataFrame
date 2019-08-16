import pandas as pd
import numpy as np
from datetime import datetime, timedelta

days = pd.date_range("1970-01-01", "2019-08-15", freq='1S')
normal = np.random.normal(size=len(days))
log_normal = np.random.lognormal(size=len(days))
exponential = np.random.exponential(size=len(days))
df = pd.DataFrame({"date": days, "normal": normal, "log_normal": log_normal, "exponential": exponential})
df.set_index("date")
df["normal"].mean()
df["log_normal"].mean()
df["exponential"].mean()
