# Worksheet #1
## Parallel Databases x Map-Reduce April 24th, 2019
Based on the following papers:
1. [Pavlo et al., SIGMOD 2009] A Comparison of Approaches to Large-Scale Data Analysis
2. [Dean and Ghemawat, CACM 2010] MapReduce: A Flexible Data Processing Tool
3. [Stonebraker et al., 2010] mapReduce and Parallel DBMSs: friends or foes?

---
## Questions

### 1) Does it make sense comparing map-reduce approaches with parallel and dsitributed database systems? Explain your answer.
Yes it does, since there are contexts and problems were both approaches can be used, each one bringing its advantages and disavantages to the problem at hand. _(See if there is any example on papers)_

### 2) What are the advantages of map-reduce over parallel and distributed databases?
There are several advantages of map-reduce over parallel and distributed databases, namely:
* **Inferior data loading time**, since there isn't any kind of preprocessing of the data, whilst there is on parallel and distributed databases;
* **More fault-tolerant** then its counterpart, as seen in [1];
* **No schema is enforced**, as the data does not need to follow any kind of schema;
* **Allows for more complex data interrogation** while in parallel and distributed databases the SQL language can be a restriction in the type of queries that can be made.

### 3) What are the advantages of parallel and distributed databases over map-reduce?
There are several advantages of parallel and distributed databases over map-reduce, namely:
* **Use of indexes** greatly improving query time when compared to the map-reduce approach;
* **Works with compressed data**, making the nodes access to data faster;
* **Automatic optimizations in task distribution** - the system would distrbute tasks in such way that the flux of data between nodes is minimized; 
* **Schema is enforced** wich might be good in certain situtations _(Say when)_
* **Faster data interrogation**, complex queries in the map-reduce approach are simplified by the usage of the SQL language.

### 4) What kind of operations are allowed in parallel and distributed databases that are not available “out-of-the-box” in map-reduce?
### 5) What kind of operations are allowed in map-reduce that are not available “out-of-the-box” in map-reduce?
### 6) Are there alternatives to Google’s mapreduce? How do they perform?
### 7) Search the web for parallel and distributed databases. In which situations would be interesting to use such solutions instead of using google’s mapreduce solution?

---

### Authors
* Afonso Pinto
* Edgar Carneiro
