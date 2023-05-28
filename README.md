# Analysis of Ethereum Transactions and Smart Contracts
Analysed the full set of transactions which have occurred on the Ethereum network; from the first transactions in August 2015 till January 2019. Created several Spark programs to perform multiple types of computation. presented a clear programs alongside an explnation of how the results are obtained.
# Dataset overview
Ethereum is a blockchain based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mint their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralised, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.
# Dataset Schema - blocks
number: The block number

hash: Hash of the block

parent_hash: Hash of the parent of the block

nonce: Nonce that satisfies the difficulty target

sha3_uncles: Combined has of all uncles for a given parent

logs_bloom: Data structure containing event logs

transactions_root: Root hash of the transactions in the payload

state_root: Root hash of the state object

receipts_root: hash of the transaction receipts tree

miner: The address of the beneficiary to whom the mining rewards were given

difficulty: Integer of the difficulty for this block

total_difficulty: Total difficulty of the chain until this block

size: The size of this block in bytes

extra_data: Arbitrary additional data as raw bytes

gas_limit: The maximum gas allowed in this block

gas_used: The total used gas by all transactions in this block

timestamp: The timestamp for when the block was collated

transaction_count: The number of transactions in the block

base_fee_per_gas: Base fee value
# Dataset Schema - transactions
hash: Hash of the block

nonce: Nonce that satisfies the difficulty target

block_hash: Hash of the block where the transaction is in

block_number: Block number where this transaction was in

transaction_index: Transactions index position in the block.
from_address: Address of the sender

to_address: Address of the receiver. null when it is a contract creation transaction

value: Value transferred in Wei (the smallest denomination of ether)

gas: Gas provided by the sender

gas_price : Gas price provided by the sender in Wei

input: Extra data for Ethereum functions

block_timestamp: Timestamp the associated block was registered at (effectively timestamp of the transaction)

max_fee_per_gas: Sum of base fee and max priority fee

max_priority_fee_per_gas: Tip for mining the transaction

transaction_type: Value used to indicate if the transaction is related to a contract or other specialised transaction
# Dataset Schema - contracts
address: Address of the contract

bytecode: Code for Ethereum Contract

function_sighashes: Function signature hashes of a contract

is_erc20: Whether this contract is an ERC20 contract
# Dataset Schema - scams.json
id: Unique ID for the reported scam

name: Name of the Scam

url: Hosting URL

coin: Currency the scam is attempting to gain

category: Category of scam - Phishing, Ransomware, Trust Trade, etc.

subcategory: Subdivisions of Category

description: Description of the scam provided by the reporter and datasource

addresses: List of known addresses associated with the scam

reporter: User/company who reported the scam first

ip: IP address of the reporter

status: If the scam is currently active, inactive or has been taken offline
# Part A
# PartA1:-“Monthly Transaction Volume: Bar Plot Analysis"
**AIM**:- Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.
**Methodology** 
1) Initially creating a spark RDD by reading the transactions data from the aws s3 bucket. 
2) This RDD is checked for any malformed linears using the clean transactions function that filters invalid transactions from the dataset which do not have a length of 15. A new RDD is created ‘time_epoch’ which extracts the timestamp value at index 11.
3) Year and month of a ‘block_timestamp’ variable from the clean_transactions RDD  is extracted using the ‘strftime’ function which converts the timestamp value into a desired year and month format and the ‘gmtime’ function converts the integer timestamp value to UTC. The second member of the tuple is a constant value of 1, representing the number of transactions for that month.
4) Transaction_values RDD is created which contains the count of transactions occurring each month  using the reduceByKey transformation. Here the reduceByKey function groups the tuple ‘yyyy-mm’ key  and sums the number of transactions occurring each month.
5) Finally plotted a bar plot from the output in the format (‘yyyy-mm’,transaction count value) using python and matplotlib library. 
![image](https://github.com/lasyaMundrathi/Ethereum-Analysis-and-smart-contracts-using-pyspark/assets/98383338/9ce5df3d-c295-42ac-8395-e2f3fa58980e)
# Part A2:-"Average Monthly Transaction Value: Bar Plot Analysis"
## Aim:-Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.


