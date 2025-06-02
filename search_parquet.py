import pandas as pd
import sys

def search_parquet(parquet_file, column=None, value=None, contains=None, limit=10):
    print(f"Loading parquet file: {parquet_file}")
    
    try:
        df = pd.read_parquet(parquet_file)
        print(f"Total rows: {len(df):,}")
        print(f"Total columns: {len(df.columns)}")
        print()
        
        if column is None and value is None and contains is None:
            print("Column names:")
            for i, col in enumerate(df.columns, 1):
                print(f"{i:2}. {col}")
            print()
            print(f"First {limit} rows:")
            print(df.head(limit))
            return
        
        if column and column not in df.columns:
            print(f"Error: Column '{column}' not found!")
            print("Available columns:")
            for col in df.columns:
                print(f"  - {col}")
            return
        
        if column and value:
            filtered_df = df[df[column] == value]
            print(f"Exact match for {column} = '{value}': {len(filtered_df):,} rows")
        elif column and contains:
            filtered_df = df[df[column].str.contains(str(contains), case=False, na=False)]
            print(f"Contains search for {column} containing '{contains}': {len(filtered_df):,} rows")
        else:
            print("Please specify both column and value/contains parameters")
            return
        
        if len(filtered_df) > 0:
            print(f"\nFirst {min(limit, len(filtered_df))} results:")
            print(filtered_df.head(limit))
        else:
            print("No results found!")
            
    except Exception as e:
        print(f"Error: {e}")

def interactive_search():
    parquet_file = "OP_DTL_GNRL_PGYR2023_P01302025_01212025.parquet"
    
    print("=== Parquet Data Search Tool ===")
    print("Commands:")
    print("  info - Show basic info and column names")
    print("  search <column> <value> - Exact search")
    print("  contains <column> <text> - Text contains search")
    print("  exit - Quit")
    print()
    
    try:
        df = pd.read_parquet(parquet_file)
        print(f"Loaded: {len(df):,} rows, {len(df.columns)} columns")
        print()
    except Exception as e:
        print(f"Error loading file: {e}")
        return
    
    while True:
        try:
            cmd = input("Enter command: ").strip().split()
            
            if not cmd or cmd[0] == "exit":
                break
            elif cmd[0] == "info":
                search_parquet(parquet_file)
            elif cmd[0] == "search" and len(cmd) >= 3:
                column = cmd[1]
                value = " ".join(cmd[2:])
                search_parquet(parquet_file, column=column, value=value)
            elif cmd[0] == "contains" and len(cmd) >= 3:
                column = cmd[1]
                text = " ".join(cmd[2:])
                search_parquet(parquet_file, column=column, contains=text)
            else:
                print("Invalid command. Use: info, search <column> <value>, contains <column> <text>, or exit")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
    
    print("Goodbye!")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        interactive_search()
    elif len(sys.argv) >= 2 and sys.argv[1] == "info":
        search_parquet("OP_DTL_GNRL_PGYR2023_P01302025_01212025.parquet")
    elif len(sys.argv) >= 4 and sys.argv[1] == "search":
        column = sys.argv[2]
        value = " ".join(sys.argv[3:])
        search_parquet("OP_DTL_GNRL_PGYR2023_P01302025_01212025.parquet", column=column, value=value)
    elif len(sys.argv) >= 4 and sys.argv[1] == "contains":
        column = sys.argv[2]
        text = " ".join(sys.argv[3:])
        search_parquet("OP_DTL_GNRL_PGYR2023_P01302025_01212025.parquet", column=column, contains=text)
    else:
        print("Usage:")
        print("  python search_parquet.py - Interactive mode")
        print("  python search_parquet.py info - Show file info")
        print("  python search_parquet.py search <column> <value> - Exact search")
        print("  python search_parquet.py contains <column> <text> - Contains search") 