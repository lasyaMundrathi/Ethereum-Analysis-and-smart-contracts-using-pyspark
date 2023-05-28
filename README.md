# Analysis of Ethereum Transactions and Smart Contracts
analyse the full set of transactions which have occurred on the Ethereum network; from the first transactions in August 2015 till January 2019. You will create several Spark programs to perform multiple types of computation. You will submit a report containing your results alongside an explanation of how they were obtained.
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


