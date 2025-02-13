import random
from datetime import datetime, timedelta
import csv
import os

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

if __name__ == "__main__":
    output_file = "sample_trades.csv"
    print(f"Starting to generate {output_file}...")
    create_large_trade_file(output_file)
    print("File generation complete!")