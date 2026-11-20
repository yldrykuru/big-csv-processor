import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def csv_to_parquet(csv_path, parquet_path, chunk_size=50_000):
    writer = None
    total = 0
    try:
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str, low_memory=False):
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression="zstd")
            writer.write_table(table)
            total += len(chunk)
            print(f"{total:,} rows written")
    finally:
        if writer is not None:
            writer.close()
    return total


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Stream a large CSV into a single Parquet file")
    p.add_argument("csv")
    p.add_argument("parquet")
    p.add_argument("--chunk-size", type=int, default=50_000)
    a = p.parse_args()
    csv_to_parquet(a.csv, a.parquet, a.chunk_size)
