# xact-mongodb-impl
Trade transaction system implemented by MongoDB

## Repository
[xact-mongodb-impl](https://github.com/DdMad/xact-mongodb-impl)

## Setup
Run `gradle` build to download dependencies and `gradle` version is 2.5

Or you can choose to use the compiled executable jar file

For setting up MongoDB, after you install MongoDB, create folder for data and log by:
```bash
mkdir {data directory path}
mkdir {log directory path}
```
For our setup, we install `MongoDB` in `/temp/mongodb/`, and data directory path is `/temp/mongodb/data` and log directory path is `/temp/mongodb/log`

### For 1 node setup
Just start `mongod` by:
```bash
/temp/mongodb/bin/mongod --fork --dbpath={data directory path} --logpath={log directory path}/{log file name}
```
Since our program uses default port number for MongoDB, you may not change it.

### For 3 node setup
For using 3 node, make sure the `mongod` for 1 node is stoppd, and then start `mongod` for each node used:
```bash
/temp/mongodb/bin/mongod --port 30001 --fork --dbpath={data directory path} --logpath={log directory path}/{log file name}
```
Also, you can change the port number above(i.e. 30001) to other available port number.

Then, you need to pick up one node to run the `mongos` and `config server`. For this, we run the `mongos` and `config server` on xcnd3, run `mongod`(i.e. shard) on xcnd3, xcnd4, xcnd5.

Then start `config server` by:
```bash
/temp/mongodb/bin/mongod --configsvr --port 30002 --fork --dbpath={data directory path} --logpath={log directory path}/{log file name} 
```
Also, you can change the port number above(i.e. 30002) if you want.

Then start `mongos` by:
```bash
/temp/mongodb/bin/mongos --fork --configdb {config server address}:{config server port number} --logpath={log directory path}/{log file name} 
```
For this, we use the default port number and you may not change it otherwise the program may fail.

Now we have set up 3 `mongod` (i.e. shard) and 1 `mongos` and 1 `config server`.

For configure shard, first use mongo client to connect `mongos` by:
```bash
/temp/mongodb/bin/mongo
```
If you change the port number of mongos, then you need to specify the port number here by `--port {your port number}`

Then add shards by:
```bash
mongos> use admin
mongos> db.runCommand({addshard : "xcnd3.comp.nus.edu.sg:30001", allowLocal : true})
mongos> db.runCommand({addshard : "xcnd4.comp.nus.edu.sg:30001", allowLocal : true})
mongos> db.runCommand({addshard : "xcnd5.comp.nus.edu.sg:30001", allowLocal : true})
```
You can specify other nodes by changing the address and port number above (i.e. "xcnd3.comp.nus.edu.sg:30001")

If you want to run single node after running 3 nodes, you should remove other shards by running:
```bash
mongos> use admin
mongos> db.runCommand({removeShard : "shard0001"})
mongos> db.runCommand({removeShard : "shard0002"})
```
Before using these commands, make sure the databases that enables sharding on those shards are dropped; otherwise error may occur.

## Load Data
After you set up with Intellij or some toher IDE, you can run `Loader` class to load data into database. 

In addition, you can just run jar file with following command:
### Load D8 database:
```bash
java -cp mongodb-impl.jar Loader "d8" 1
```
### Load D40 database:
```bash
java -cp mongodb-impl.jar Loader "d40" 1
```
Here all the data csv files are in a folder named **d8** (for D8 data files) or **d40** (for D40 data files)

The second argument is the number of node used. The above example shows if single node is used. If 3 nodes are used, then use following command:
### Load D8 database using 3 node:
```bash
java -cp mongodb-impl.jar Loader "d8" 3
```
### Load D40 database using 3 node:
```bash
java -cp mongodb-impl.jar Loader "d40" 3
```
If 3 nodes are used, then MongoDB sharding will be used.

## Run Transaction
After loading data, you can run one client using following command:

### Run D8 Transactions
```bash
java -cp mongodb-impl.jar Processor "d8" "0.txt"
```
### Run D40 Transactions
```bash
java -cp mongodb-impl.jar Processor "d40" "0.txt"
```

The text file `0.txt` can be changed to other transaction files. Here the transaction files (e.g. 0.txt) must be in a folder called **d8-xact** (for D8 xact files) or **d40-xacy** (for D40 xact files).

Or you may want to modify the Java class. After your modification, you can compile the whole project to a executable jar file and then run it.

## Output
All the transaction result will be written to a file. For example, for transaction file `0.txt`, the default output is `0.txt-out.txt`. You may want to change this format in the XactProcessor.java.

The final result of thoughput will be written in a file called `result-d8-n.txt.out` or `result-d40-n.txt.out`, where `n` is the number of transaction file.

All the output files will be in the same directory as the data files or xact files.

## PS
When you run the jar, ensure that there is a `log4j.properities`. If you run the jar in other places, just copy the `log4j.properities` in this project to the same place.

