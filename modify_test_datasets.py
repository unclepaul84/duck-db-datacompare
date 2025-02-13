import random
import csv
import os
import tempfile
from itertools import islice

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
    modify_random_prices("sample_trades_2.csv", 1000000)