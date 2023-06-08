# Analysis of Ethereum Transactions and Smart Contracts
Analysed the full set of transactions which have occurred on the Ethereum network; from the first transactions in August 2015 till January 2019. Created several Spark programs to perform multiple types of computation. presented a clear programs alongside an explnation of how the results are obtained.
### Dataset overview
Ethereum is a blockchain based distributed computing platform where users may exchange currency (Ether), provide or purchase services (smart contracts), mint their own coinage (tokens), as well as other applications. The Ethereum network is fully decentralised, managed by public-key cryptography, peer-to-peer networking, and proof-of-work to process/verify transactions.
### Dataset Schema - blocks
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
### Dataset Schema - transactions
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
### Dataset Schema - contracts
address: Address of the contract

bytecode: Code for Ethereum Contract

function_sighashes: Function signature hashes of a contract

is_erc20: Whether this contract is an ERC20 contract
### Dataset Schema - scams.json
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
## Part A
#### PartA1:-“Monthly Transaction Volume: Bar Plot Analysis"
**Objective**:- Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

**Methodology** 
1) Initially creating a spark RDD by reading the transactions data from the aws s3 bucket. 
2) This RDD is checked for any malformed linears using the clean transactions function that filters invalid transactions from the dataset which do not have a length of 15. A new RDD is created ‘time_epoch’ which extracts the timestamp value at index 11.
3) Year and month of a ‘block_timestamp’ variable from the clean_transactions RDD  is extracted using the ‘strftime’ function which converts the timestamp value into a desired year and month format and the ‘gmtime’ function converts the integer timestamp value to UTC. The second member of the tuple is a constant value of 1, representing the number of transactions for that month.
4) Transaction_values RDD is created which contains the count of transactions occurring each month  using the reduceByKey transformation. Here the reduceByKey function groups the tuple ‘yyyy-mm’ key  and sums the number of transactions occurring each month.
5) Finally plotted a bar plot from the output in the format (‘yyyy-mm’,transaction count value) using python and matplotlib library. 

**Output**

![image](https://github.com/lasyaMundrathi/Ethereum-Analysis-and-smart-contracts-using-pyspark/assets/98383338/9ce5df3d-c295-42ac-8395-e2f3fa58980e)

#### Part A2:-"Average Monthly Transaction Value: Bar Plot Analysis"
**Objective:-Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.**

**Methodology**

1) The code in average transactions calculates the monthly average of transaction values occurring each month over time. 
2) The clean transactions RDD is mapped using a lambda function to build a new RDD containing tuples of the type (block timestamp, value). The split technique is used to extract the date and value from each line of the input RDD.
3) The resultant RDD is mapped again using a lambda function to produce a new RDD containing tuples of the type (YYYY-MM, value), where YYYY-MM is the year and month corresponding to the timestamp value retrieved in the previous step. The ‘strftime’ function is used to transform a timestamp into a string with the chosen date format.
4) The generated RDD is grouped by key (i.e., by year and month) using the groupByKey transformation.
5) To compute the average of the values for each key, the mapValues transformation is used to the grouped RDD. The sum and len functions are used to compute the sum of the values for each key and the number of values. After then, the average is calculated by dividing the total by the number of values.
6) Finally, the RDD is allocated to the variable monthly avg and reported to the console.

**Output:**

![image](https://github.com/lasyaMundrathi/Ethereum-Analysis-and-smart-contracts-using-pyspark/assets/98383338/5e64a0aa-5210-4cf9-ad58-cc0f8af7e92c)
The conclusion from the preceding graph is that the average transaction value was greatest during the early days of the dataset owing to the low number of transactions; nevertheless, as the crypto buzzword expanded over the world, the ethereum transaction counts rapidly climbed during the years, resulting in a dropping average transaction value.
## Part B.Top Ten Most Popular Services
**Objective :-** To assess the top ten smart contracts in terms of total Ether received. This involves connecting the contracts dataset's **address field** to the transactions dataset's **to_address** to calculate how much ether a contract has received.

The algorithm seeks to analyse the top ten most popular services by locating the contract addresses with the greatest total transaction values. The resultant top10 RDD is a series of tuples, the first of which is the address of a contract and the second of which is the total of values received by that contract. 

**Methodology:-**
1) The contract's RDD is built by reading a CSV file from an S3 bucket using the Spark context object's textFile function.
2) The clean contracts RDD is built by filtering the contracts RDD to retain just the valid contract lines.
3) The contract address RDD is built by mapping the clean contracts RDD to extract the address from each contract line and then associating it with the string "contract."
4) The transaction's RDD is formed by reading the csv file from the aws s3 bucket.  The transaction to address RDD is formed by extracting the ‘to_address’ and ‘value’ data from each transaction line, and the clean transactions RDD is created by filtering the valid transaction lines.
5) The transaction's RDD is constructed by summarising the values for each unique to_ address using the reduceByKey transformation on the transaction to address RDD.
6) The contract data RDD is formed by combining the data from the transactions and contract address RDDs using the join transformation from both RDDs based on the common key (i.e., address).
7) The top10 RDD is generated by performing the takeOrdered transformation on the contract data RDD in order to acquire the top ten most popular services. The takeOrdered function's key parameter sets the sorting criterion, which in this case is the first member of each tuple in the contract data RDD (i.e., the sum of values for each contract address).
Finally top10 values are extracted.

**Output:**
![image](https://github.com/lasyaMundrathi/Ethereum-Analysis-and-smart-contracts-using-pyspark/assets/98383338/6b29851c-c8d7-4dd0-aa97-6f0f541a7575)



<div style="width: 50px;">
  
| Rank | Address | Value |
|----------|----------|----------|
| 1    | "0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444"  | 84155363699941767867374641  |
| 2    | "0x7727e5113d1d161373623e5f49fd568b4f543a9e"| 45627128512915344587749920   |
| 3    | "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef"   | 4255298913641319891929896 |
| 4    | 0xfa52274dd61e1643d2205169732f29114bc240b3 | 40546128459947291326220872   |
| 5    | 0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8   |24543161734499779571163970   |
| 6    | 0xbfc39b6f805a9e40e77291aff27aee3c96915bdd   | 21104195138093660050000000   |
|   7  | 0xe94b04a0fed112f3664e45adb2b8915693dd5ff3   | 15543077635263742254719409   |
| 8    | 0xbb9bc244d798123fde783fcc1c72d3bb8c189413   | 11983608729102893846818681   |
| 9   | 0xabbb6bebfa05aa13e908eaa492bd7a8343760477   | 10719485945628946136524680  |
| 10    | 0x341e790174e3a4d35b65fdc067b6b5634a61caea   |8379000751917755624057500   |
</div>

**Part C Top Ten Most Active Miners**
**Objective :** The algorithm seeks to analyse the top ten most popular services  by the size of the blocks mined.
## **Methodology**
1) The block's RDD is created by utilising the textFile method of the Spark context object to read a CSV file from an S3 bucket.
2) By retaining just the valid block lines from the blocks RDD, the clean blocks RDD is created.
3) The miner and size fields from each block line are mapped to the clean blocks RDD to create the block to address RDD. Index 9 is used to retrieve the miner field from the comma-separated text, while index 12 is used to extract the size field.
4) By adding the size values for each miner and performing a reduceByKey operation on the block to address RDD, the miners RDD is created.
5) Next, using the sortBy method with False as the second argument to denote a decreasing order, the miners RDD is sorted by size in decreasing order.
6) In order to gather the top 10 miners by size, the topminers variable is created by invoking the miners RDD's takeOrdered function. The takeOrdered method delivers the top ten records in decreasing order of size since the key parameter is set to a lambda function that returns the negative value of each record's size.
7) The variable topminers, which contains a list of tuples containing the addresses and aggregate size of the top 10 miners, is written to the console.

**Output:**
![image](https://github.com/lasyaMundrathi/Ethereum-Analysis-and-smart-contracts-using-pyspark/assets/98383338/4b76af0e-bc78-4442-affe-939b4bf3f05b)

<div style="width: 50px;">
  
| Rank | Address | Value |
|----------|----------|----------|
| 1    | 0xea674fdde714fd979de3edf0f56aa9716b898ec8  | 17453393724 |
| 2    | 0x829bd824b016326a401d083b33d092293333a830| 12310472526   |
| 3    | "0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c"   | 8825710065 |
| 4    | 0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5 | 8451574409   |
| 5    | 0xb2930b35844a230f00e51431acae96fe543a0347   |6614130661   |
| 6    | 0x2a65aca4d5fc5b5c859090a6c34d164135398226   | 3173096011   |
|   7  | 0xf3b9d2c81f2b24b0fa0acaaa865b7d9ced5fc2fb   | 1152847020   |
| 8    | 0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01   | 1134151226   |
| 9   | 0x1e9939daaad6924ad004c2560e90804164900341   | 1080436358  |
| 10    | 0x61c808d82a3ac53231750dadc13c777b59310bd9   |692942577   |
  
</div>


