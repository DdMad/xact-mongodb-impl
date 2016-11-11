# xact-mongodb-impl
Trade transaction system implemented by MongoDB

## Repository
[xact-mongodb-impl](https://github.com/DdMad/xact-mongodb-impl)

## Setup
Run `gradle` build to download dependencies

Or you can choose to use the compiled executable jar file

## Load Data
After you set up with Intellij or some toher IDE, you can run `Loader` class to load data into database. In additon, you can configure the path of the `schema` file and the `data` file inside the class.

In addition, you can just run jar file with following command:
### Load D8 database:
```bash
java -cp mongodb-impl.jar Loader "d8"
```
### Load D40 database:
```bash
java -cp mongodb-impl.jar Loader "d40"
```
Here all the data csv files are in a folder named **d8** (for D8 data files) or **d40** (for D40 data files)

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

