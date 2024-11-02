# tsimporter

```python
import datetime
import random
from pathlib import Path

from tsdataset import ParquetImporter

number_of_data = 3000
df = pd.DataFrame()
df["time"] = pd.date_range("2021-01-01", periods=number_of_data, freq=datetime.timedelta(hours=1))
df["data1"] = [random.random() for _ in range(number_of_data)]
df["label"] = [True if random.random() > 0.5 else False for _ in range(number_of_data)]


df.iloc[:1000].to_parquet(sample_dir / "data1.parquet")
df.iloc[1000:2000].to_parquet(sample_dir / "data2.parquet")
df.iloc[2000:].to_parquet(sample_dir / "data3.parquet")

conn_params = {
    "host": "localhost",
    "port": "5432",
    "dbname": "dbname",
    "user": "user",
    "password": "password",
}
table_name = "table"
column_mappings = [
    {"source": "time", "target": "time", "dtype": "TIMESTAMP", "primary_key": True},
    {"source": "data1", "target": "v1", "dtype": "FLOAT"},
    {"source": "label", "target": "flag", "dtype": "BOOL"},
]
importer = ParquetImporter(conn_params=conn_params, table_name=table_name, column_mappings=column_mappings)
importer.import_files(["data1.parquet", "data2.parquet", "data3.parquet"])
```