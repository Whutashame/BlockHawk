from threading import Thread, Lock
from queue import Queue
from web3 import Web3
import mysql.connector
import requests
import time
import warnings
import subprocess
from web3.exceptions import MismatchedABI

warnings.filterwarnings("ignore", category=UserWarning, module="web3")

web3 = Web3(Web3.HTTPProvider('https://eth-mainnet.alchemyapi.io/v2/s6cYdhFdblEUj-IzCx3BmiX9-GTVAfOJ'))

erc20_abi = [
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    }
]

generic_erc20_abi = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"}
        ],
        "name": "Transfer",
        "type": "event"
    }
]

price_cache = {}
queue = Queue()
lock = Lock()

# Rate limiting parameters
RATE_LIMIT = 5  # Number of requests per second
LAST_REQUEST_TIME = time.time()

def rate_limit():
    global LAST_REQUEST_TIME
    current_time = time.time()
    elapsed_time = current_time - LAST_REQUEST_TIME
    sleep_time = max(0, 1 / RATE_LIMIT - elapsed_time)
    time.sleep(sleep_time)
    LAST_REQUEST_TIME = time.time()

def get_token_price(token_symbol):
    if token_symbol in price_cache:
        return price_cache[token_symbol]
    
    api_key = 'b9e63fd80265ffda604b5ec07657a02097e153d4a90a01af677e2fee4e1a0687'  
    try:
        url = f"https://min-api.cryptocompare.com/data/price?fsym={token_symbol}&tsyms=ETH&api_key={api_key}"
        response = requests.get(url)
        price_data = response.json()
        price_cache[token_symbol] = price_data.get('ETH', 0)
        return price_cache[token_symbol]
    except:
        return 0

def process_transactions(txs, block_number, block_timestamp):
    for tx in txs:
        try:
            rate_limit()  # Ensure we respect the rate limit

            tx_hash = tx['hash'].hex()
            tx_to = tx['to']
            tx_from = tx['from']
            value = Web3.from_wei(tx['value'], 'ether')
            tx_receipt = web3.eth.get_transaction_receipt(tx_hash)
            token = "ETH"
            token_value = 0
            token_decimals = 18
            token_value_in_eth = 0

            try: 
                token_contract = web3.eth.contract(address=tx_to, abi=erc20_abi)
                token = token_contract.functions.symbol().call()
                token_decimals = token_contract.functions.decimals().call()
            except:
                pass

            if token != "ETH":
                generic_contract = web3.eth.contract(address=tx_to, abi=generic_erc20_abi)
                try:
                    transfer_events = generic_contract.events.Transfer().process_receipt(tx_receipt)
                    if transfer_events:
                        token_value_raw = transfer_events[0]['args']['value']
                        token_value = token_value_raw / (10 ** token_decimals)
                        token_price_in_eth = get_token_price(token)
                        if token_price_in_eth != 0:
                            token_value_in_eth = token_value * token_price_in_eth
                except MismatchedABI:
                    pass

            if value != 0 or token_value != 0:
                with lock:
                    queue.put((tx_hash, tx_from, tx_to, value, token, token_value, token_value_in_eth, block_number, block_timestamp))
        except Exception as e:
            print(f"Error processing transaction {tx['hash'].hex()}: {e}")

def process_block(block_number):
    db_connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="",
        database="pfs"
    )
    cursor = db_connection.cursor()
    try:
        block = web3.eth.get_block(block_number, full_transactions=True)
        block_timestamp = block['timestamp']
        print(f"Transactions are being stored from block {block_number}:")

        txs = block.transactions
        tx_chunks = [txs[i:i + 10] for i in range(0, len(txs), 10)]

        threads = []
        for chunk in tx_chunks:
            thread = Thread(target=process_transactions, args=(chunk, block_number, block_timestamp))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        while not queue.empty():
            tx_hash, tx_from, tx_to, value, token, token_value, token_value_in_eth, block_number, block_timestamp = queue.get()
            cursor.execute("""
                INSERT INTO transactions3 (tx_hash, tx_from, tx_to, value, token, t_value, t_value_in_eth, block_timestamp, block_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (tx_hash, tx_from, tx_to, value, token, token_value, token_value_in_eth, block_timestamp, block_number))

        db_connection.commit()

        subprocess.run(["python", "score.py"])

    except Exception as e:
        print(f"Error processing block {block_number}: {e}")
    finally:
        cursor.close()
        db_connection.close()

def poll_for_new_blocks():
    latest_block = web3.eth.get_block('latest').number
    while True:
        rate_limit()  # Ensure we respect the rate limit
        current_block = web3.eth.get_block('latest').number
        if current_block > latest_block:
            for block_number in range(latest_block + 1, current_block + 1):
                process_block(block_number)
            latest_block = current_block
        time.sleep(10)  # Check for a new block every 10 seconds

if __name__ == "__main__":
    poll_for_new_blocks()
