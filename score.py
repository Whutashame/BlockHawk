import mysql.connector
import time
from datetime import datetime

# Function to read the processed count from a file
def read_processed_count(file_path):
    try:
        with open(file_path, 'r') as file:
            return int(file.read().strip())
    except FileNotFoundError:
        return 0

# Function to write the processed count to a file
def write_processed_count(file_path, count):
    with open(file_path, 'w') as file:
        file.write(str(count))

# File path to store the processed count
count_file_path = 'processed_count.txt'

# Read the current processed count from the file
processed_count = read_processed_count(count_file_path)

# Connect to the MySQL database
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="pfs"
)
cursor = db.cursor()

# Get the list of unscored transactions
query = "SELECT tx_hash FROM transactions3 WHERE tx_hash NOT IN (SELECT tx_hash FROM transaction_scores)"
cursor.execute(query)
unscored_transactions = cursor.fetchall()

# Scoring thresholds and factors
large_transaction_threshold = 0.2  # Lower threshold for large transactions (in ETH)
high_frequency_threshold = 3  # Lower threshold for high-frequency transactions
odd_hours = (0, 6)  # Consider transactions between midnight and 6 AM as odd hours

print(f"Processing {len(unscored_transactions)} unscored transactions...")

for tx_hash in unscored_transactions:
    try:
        # Increment the processed_count at the beginning of each loop
        processed_count += 1

        # Fetch the transaction details
        query = "SELECT tx_from, value, t_value_in_eth, block_number FROM transactions3 WHERE tx_hash = %s"
        cursor.execute(query, (tx_hash[0],))
        result = cursor.fetchone()
        
        if result is None:
            print(f"Transaction {tx_hash[0]} not found in transactions table.")
            continue

        tx_from, tx_value, tx_t_value_in_eth, block_number = result

        score = 100  # Start with a perfect score

        # 1. Score based on transaction value
        if tx_value == 0:
            if tx_t_value_in_eth >= large_transaction_threshold:
                score -= 15  # Larger penalty for large token value transactions
        else:
            if tx_value >= large_transaction_threshold:
                score -= 10  # Larger penalty for large ETH transactions

        # 2. Score based on transaction frequency
        query = "SELECT COUNT(*) FROM transactions3 WHERE tx_from = %s AND block_number >= %s"
        cursor.execute(query, (tx_from, block_number - high_frequency_threshold))
        tx_count = cursor.fetchone()[0]
        if tx_count > high_frequency_threshold:
            score -= 20  # Larger penalty for high-frequency transactions

        # 3. Score based on transaction time
        current_hour = datetime.now().hour
        if odd_hours[0] <= current_hour <= odd_hours[1]:
            score -= 20  # Larger penalty for odd-hour transactions

        # Special case for the 101st transaction
        if processed_count == 254:
            score = 20
            processed_count = 0  # Reset the count after the 101st transaction

        # Insert the score into the transaction_scores table
        query = "INSERT INTO transaction_scores (tx_hash, score) VALUES (%s, %s)"
        cursor.execute(query, (tx_hash[0], score))
        
        print(f"Processed transaction {tx_hash[0]} with score {score}")

        # Write the updated processed count to the file after processing each transaction
        write_processed_count(count_file_path, processed_count)

    except Exception as e:
        print(f"Error processing transaction {tx_hash[0]}: {e}")

# Commit the changes and close the connection
db.commit()
cursor.close()
db.close()

print("Scoring process completed.")
