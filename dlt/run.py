import csv
import os
import dlt
import pyarrow as pa
import pyarrow.csv as pv

DATA_DIR = "/data"
CSV_FILE = os.path.join(DATA_DIR, "OP_DTL_GNRL_PGYR2025_P06302026_06032026.csv")

@dlt.resource(
    name="op_dtl_gnrl_pgyr2025",
    write_disposition="replace",
)
def source():
    # 1. Read ONLY the first line (headers) without loading the dataset into RAM
    with open(CSV_FILE, mode="r", encoding="utf-8-sig") as f:
        headers = next(csv.reader(f))

    # 2. Force every column to string type to prevent ArrowNull / inference crashes
    column_types = {col: pa.string() for col in headers}

    # 3. Stream the file in chunks using PyArrow
    reader = pv.open_csv(
        CSV_FILE,
        read_options=pv.ReadOptions(use_threads=True),
        convert_options=pv.ConvertOptions(column_types=column_types),
    )

    for batch in reader:
        yield batch

pipeline = dlt.pipeline(
    pipeline_name="csv_to_db",
    destination=dlt.destinations.postgres("postgresql://dm_postgres:dm_postgres@postgres:5432/dm_postgres"),
    dataset_name="raw",
    progress="log",
)

load_info = pipeline.run(source())