import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

chunk_size = 50000

csv_file = "OP_DTL_GNRL_PGYR2023_P01302025_01212025.csv"
parquet_file = "OP_DTL_GNRL_PGYR2023_P01302025_01212025.parquet"
temp_dir = "temp_chunks"

print(f"Starting conversion of {csv_file} to {parquet_file}")
print(f"Processing in chunks of {chunk_size:,} rows")

Path(temp_dir).mkdir(exist_ok=True)

if os.path.exists(parquet_file):
    os.remove(parquet_file)

chunk_num = 0
total_rows_processed = 0
chunk_files = []

try:
    csv_reader = pd.read_csv(
        csv_file,
        chunksize=chunk_size,
        low_memory=False,
        dtype=str
    )
    
    for chunk in csv_reader:
        chunk_num += 1
        rows_in_chunk = len(chunk)
        total_rows_processed += rows_in_chunk
        
        print(f"Processing chunk {chunk_num}: {rows_in_chunk:,} rows (Total: {total_rows_processed:,})")
        
        temp_file = f"{temp_dir}/chunk_{chunk_num:04d}.parquet"
        chunk.to_parquet(temp_file, compression="zstd", index=False)
        chunk_files.append(temp_file)
        
        del chunk
        
        if chunk_num % 10 == 0:
            print(f"Completed {chunk_num} chunks...")

    print(f"\nCombining {len(chunk_files)} chunk files into final parquet file...")
    
    tables = []
    print("Reading chunk files...")
    
    for i, chunk_file in enumerate(chunk_files):
        if (i + 1) % 20 == 0:
            print(f"Reading chunk file {i + 1}/{len(chunk_files)}")
        table = pq.read_table(chunk_file)
        tables.append(table)
    
    print("Concatenating all chunks...")
    final_table = pa.concat_tables(tables, promote_options='default')
    pq.write_table(final_table, parquet_file, compression="zstd")
    
    print(f"\nConversion complete!")
    print(f"Total rows processed: {total_rows_processed:,}")
    print(f"Output file: {parquet_file}")
    
    if os.path.exists(parquet_file):
        file_size = os.path.getsize(parquet_file) / (1024**3)
        print(f"Output file size: {file_size:.2f} GB")
    
    print("Cleaning up temporary files...")
    for chunk_file in chunk_files:
        os.remove(chunk_file)
    os.rmdir(temp_dir)
    print("Cleanup complete!")

except Exception as e:
    print(f"Error during conversion: {str(e)}")
    print("Try reducing the chunk_size if you're still running out of memory")
    try:
        for chunk_file in chunk_files:
            if os.path.exists(chunk_file):
                os.remove(chunk_file)
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)
    except:
        pass