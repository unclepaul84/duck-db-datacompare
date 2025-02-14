import random
from datetime import datetime, timedelta
import csv
import os
import shutil
import random
import csv
import os
import tempfile
from itertools import islice


trade_id = 1

def generate_random_trade():
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'BAC', 'WMT']
    trade_types = ['BUY', 'SELL']
    
    symbol = random.choice(symbols)
    price = round(random.uniform(10, 1000), 2)
    quantity = random.randint(1, 10000)
    trade_type = random.choice(trade_types)
    timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
    global trade_id
    trade_id=trade_id+1
    return [
        trade_id,
        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        symbol,
        trade_type,
        str(price),
        str(quantity)
    ]

def create_large_trade_file(filename, target_size_gb=20):
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    current_size = 0
    chunk_size = 1000000  # Number of trades per chunk
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(['trade_id','timestamp', 'symbol', 'trade_type', 'price', 'quantity'])
        
        while current_size < target_size_bytes:
            # Generate chunk of trades
            trades = [generate_random_trade() for _ in range(chunk_size)]
            writer.writerows(trades)
            
            # Update current file size
            current_size = os.path.getsize(filename)
            
            # Print progress
            progress_gb = current_size / (1024 * 1024 * 1024)
            print(f"Generated {progress_gb:.2f} GB of data...")
            
            if current_size >= target_size_bytes:
                break
def estimate_total_records(filename):
    # Sample first 1000 records to get average record size
    sample_size = 1000
    total_size = os.path.getsize(filename)
    
    with open(filename, 'r') as f:
        header = next(f)
        sample = list(islice(f, sample_size))
        avg_record_size = sum(len(line) for line in sample) / len(sample)
        
    estimated_records = (total_size - len(header)) / avg_record_size
    return int(estimated_records)

def modify_random_prices(filename, num_modifications):
    # Get estimated total records and generate random positions
    total_records = estimate_total_records(filename)
    modifications = set(random.sample(range(1, total_records), num_modifications))
    
    temp_filename = tempfile.mktemp()
    chunk_size = 100000  # Process 100K records at a time
    
    with open(filename, 'r', newline='') as infile, open(temp_filename, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write header
        header = next(reader)
        writer.writerow(header)
        
        current_line = 1
        while True:
            # Read and process chunk
            chunk = list(islice(reader, chunk_size))
            if not chunk:
                break
                
            # Modify records in chunk if needed
            for row in chunk:
                if current_line in modifications:
                    row[4] = str(round(random.uniform(10, 1000), 2))
                writer.writerow(row)
                current_line += 1
    
    # Replace original file with modified version
    os.replace(temp_filename, filename)

def delete_random_rows(filename, num_deletions):
    # Get estimated total records and generate positions to delete
    total_records = estimate_total_records(filename)
    deletions = set(random.sample(range(1, total_records), num_deletions))
    
    temp_filename = tempfile.mktemp()
    chunk_size = 100000  # Process 100K records at a time
    
    with open(filename, 'r', newline='') as infile, open(temp_filename, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write header
        header = next(reader)
        writer.writerow(header)
        
        current_line = 1
        while True:
            # Read and process chunk
            chunk = list(islice(reader, chunk_size))
            if not chunk:
                break
                
            # Write rows that aren't marked for deletion
            for row in chunk:
                if current_line not in deletions:
                    writer.writerow(row)
                current_line += 1
    
    # Replace original file with modified version
    os.replace(temp_filename, filename)

if __name__ == "__main__":
    output_file = "legacy_system_trades.csv"

    print(f"Starting to generate {output_file}...")

    create_large_trade_file(output_file, target_size_gb=2)

    shutil.copy2("legacy_system_trades.csv", "new_system_trades.csv")
    print(f"Deleting randoom rows in {output_file}...")
    delete_random_rows(output_file,100000)

    print(f"Modifying randoom rows in {output_file}...")
    modify_random_prices(output_file,50000)

    print("File generation complete!")