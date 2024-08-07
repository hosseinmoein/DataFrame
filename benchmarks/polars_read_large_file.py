import datetime
import polars as pl

# ------------------------------------------------------------------------------


start = datetime.datetime.now()
df = pl.read_csv("Large_File.csv")
end = datetime.datetime.now()

print(f"Column Length: {df.select(pl.count()).item()}")
print(f"Reading Took: {(end - start).seconds}.{(end - start).microseconds}")

# ------------------------------------------------------------------------------

# Local Variables:
# mode:Python
# tab-width:4
# c-basic-offset:4
# End:
