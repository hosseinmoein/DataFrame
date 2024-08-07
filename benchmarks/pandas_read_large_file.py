import datetime
import pandas as pd

# ------------------------------------------------------------------------------

start = datetime.datetime.now()
df = df = pd.read_csv("Large_File.csv")
end = datetime.datetime.now()

print(f"Column Length: {len(df['COL6:200000000:<int>'])}")
print(f"Reading Took: {(end - start).seconds}.{(end - start).microseconds}")

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:

