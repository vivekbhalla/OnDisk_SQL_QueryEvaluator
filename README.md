OnDisk_SQL_QueryEvaluator
=========================

Query evaluator to provide full support for SQL, out-of-core query processing and limited forms of query evaluation

This project is, in effect, a more rigorous form of the project [SimpleSPJUA_QueryEvaluator](https://github.com/vivekbhalla/SimpleSPJUA_QueryEvaluator).
The implementation and input is identical and based on this project.
Query and data is given, it evaluates the query on the data and give us a response as quickly as possible.

This code supports a broader range of queries selected from TPC-H benchmark.

It performs more efficiently, and handles a lot more data that does not fit into main memory.

#### Blocking Operators and Memory

Blocking operators (e.g., joins other than Merge Join, the Sort operator, etc...) are generally blocking because
they need to materialize instances of a relation. For most of the part of this project, the implementation assumes
that there is not enough memory available to materialize a full relation, to say nothing of join results.

To successfully process these queries, we have implemented out-of core equivalents of these operators:

    An External Join algorithm called Hybrid Hash Join has been implemented for better efficiency in both 
    aspects time and memory.
    The project has been tested on evaluation machines with 2GB of memory, and Java is configured for a 1GB heap.
    The project was run on a TPCH dataset of 2GB and it had good performance.

Example invocation

    java -Xmx1024m -cp build:jsqlparser.jar edu.buffalo.cse562.Main --data [data] --swap [swap] [sqlfile1] [sqlfile2] ... 

This example uses the following directories and files

  •	[data]: Table data stored in '|' separated files. Table names match the names provided in the matching CREATE TABLE with the .dat suffix.
  •	swap: A temporary directory for an individual run.
  • [sqlfileX]: A file containing CREATE TABLE and SELECT statements, defining the schema of the dataset and the query to process
