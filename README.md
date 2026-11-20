# Big CSV Processor 🚀

**Memory-efficient CSV to Parquet converter with 94.5% compression ratio**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Pandas](https://img.shields.io/badge/Pandas-2.0%2B-red)](https://pandas.pydata.org/)
[![PyArrow](https://img.shields.io/badge/PyArrow-Latest-orange)](https://arrow.apache.org/docs/python/)

> Transform massive CSV files into lightning-fast Parquet format without running out of memory!

## 📊 The Problem

Ever tried to load a multi-gigabyte CSV file with pandas and got this?

```python
df = pd.read_csv("huge_file.csv")  # ❌ MemoryError!
```

**Big CSV Processor** solves this by using intelligent chunked processing and advanced compression techniques.

## ✨ Key Features

- 🔥 **94.5% size reduction** (7.6GB → 0.42GB in real tests)
- ⚡ **Fast columnar queries** with DuckDB or pandas
- 💾 **Memory-efficient** chunked processing
- 🔍 **Built-in search system** for instant data exploration
- 🧹 Streams each chunk into a single Parquet file with `ParquetWriter`
- 📦 **ZSTD compression** for maximum efficiency

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yldrykuru/big-csv-processor.git
cd big-csv-processor

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

```bash
# Convert CSV to Parquet
python main.py input.csv output.parquet

# Search the converted data
python search_parquet.py
```

## 📈 Real-World Results

| Metric | Before (CSV) | After (Parquet) | Improvement |
|--------|--------------|-----------------|-------------|
| **File Size** | 7.6 GB | 0.42 GB | **94.5% reduction** |
| **Memory Usage** | 8+ GB | 1.5 GB | **81% less RAM** |
| **Query Time** | 30+ seconds | <2 seconds | **15x faster** |
| **Storage Cost** | $$$$ | $ | **Massive savings** |

## 🛠️ How It Works

### 1. Chunked Processing (`main.py`)

```python
# Process large files in manageable chunks
csv_reader = pd.read_csv(
    csv_file,
    chunksize=50000,  # Optimal chunk size
    dtype=str,        # Prevent schema conflicts
    low_memory=False
)

# Convert each chunk to compressed Parquet
for chunk in csv_reader:
    chunk.to_parquet(temp_file, compression="zstd")
```

### 2. Smart Compression

- **ZSTD compression**: Best-in-class compression ratio
- **Columnar format**: Efficient data storage and querying
- **Schema optimization**: Automatic type inference

### 3. Lightning-Fast Search (`search_parquet.py`)

```bash
# Interactive mode
python search_parquet.py

# Quick searches
python search_parquet.py search column_name "value"
python search_parquet.py contains column_name "partial_text"
python search_parquet.py info  # Show file structure
```

## 📋 Requirements

```txt
pandas>=2.0.0
pyarrow>=10.0.0
```

## 🎯 Usage Examples

### Convert Any CSV File

```python
import pandas as pd
from pathlib import Path

# Modify these variables for your file
csv_file = "your_large_file.csv"
parquet_file = "output.parquet"
chunk_size = 50000

# Run the conversion
python main.py input.csv output.parquet
```

### Search Your Data

```python
# Load and explore
python search_parquet.py info

# Find specific records
python search_parquet.py search "State" "California"

# Text search
python search_parquet.py contains "Name" "Smith"
```

## 🏗️ Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Large     │───▶│   Chunked    │───▶│ Compressed  │
│   CSV       │    │  Processing  │    │  Parquet    │
│   7.6 GB    │    │              │    │   0.42 GB   │
└─────────────┘    └──────────────┘    └─────────────┘
                            │
                            ▼
                   ┌──────────────┐
                   │    Search    │
                   │    System    │
                   │   <2 sec     │
                   └──────────────┘
```

## 📊 Performance Benchmarks

### File Size Comparison
- **Original CSV**: 7.6 GB
- **Parquet (uncompressed)**: 2.1 GB (72% reduction)
- **Parquet + GZIP**: 0.8 GB (89% reduction)
- **Parquet + ZSTD**: 0.42 GB (94.5% reduction) ⭐

### Query Performance
- **CSV + pandas**: 30+ seconds
- **Parquet + pandas**: 8 seconds

## 🔧 Configuration

### Chunk Size Optimization

| RAM Available | Recommended Chunk Size |
|---------------|------------------------|
| 4 GB | 25,000 rows |
| 8 GB | 50,000 rows (default) |
| 16 GB+ | 100,000 rows |

### Compression Options

```python
# Available compression algorithms
compression_options = [
    "zstd",    # Best ratio (recommended)
    "gzip",    # Good balance
    "snappy",  # Fastest
    "lz4",     # Fast with decent ratio
]
```

## 🚀 Production Deployment

### Enterprise File Structure

```
/data/
├── raw/                 # Original CSV files
├── processed/
│   ├── 2023/
│   │   ├── 01/         # Monthly partitions
│   │   └── 02/
│   └── 2024/
├── indexes/
│   ├── metadata.sqlite # File metadata
│   └── search_cache.db # Query cache
└── backups/            # Compressed backups
```

### Docker Support

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "main.py"]
```

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **pandas** team for the amazing data processing library
- **Apache Arrow** team for the high-performance Parquet implementation

## 📚 Related Articles

- [Medium Article: From 7.6GB CSV to 0.42GB Parquet](https://yldrykuru.medium.com/from-7-6gb-csv-to-0-42gb-parquet-production-ready-big-data-solution-d09dc1f1bbd8)

## 🔗 Useful Links

- [Parquet Documentation](https://parquet.apache.org/docs/)
- [pandas Documentation](https://pandas.pydata.org/docs/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)

## 📊 Project Stats

![Project Stats](https://github-readme-stats.vercel.app/api/pin/?username=yldrykuru&repo=big-csv-processor&theme=dark)

---

⭐ **Star this repo if it helped you!** ⭐

**Made with ❤️ for the data science community** 
