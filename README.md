# PySpark_pagerank

In this assignment, Sai (ss6365) and Nickolo (nr2810) are working on a project to implement PageRank algorithm in PySpark.


## Question 1 

#### What is the default block size on HDFS? What is the default replication factor of HDFS on Dataproc?

The Default bloack size on HDFS for our instance is 128 MB. The default replication factor of HDFS on Dataproc is 2.

The Schema of the file is:

```
root
 |-- id: long (nullable = true)
 |-- redirect: string (nullable = true)
 |-- restrictions: string (nullable = true)
 |-- revision: struct (nullable = true)
 |    |-- comment: struct (nullable = true)
 |    |    |-- _VALUE: string (nullable = true)
 |    |    |-- _deleted: string (nullable = true)
 |    |-- contributor: struct (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- ip: string (nullable = true)
 |    |    |-- username: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- minor: string (nullable = true)
 |    |-- text: struct (nullable = true)
 |    |    |-- _VALUE: string (nullable = true)
 |    |    |-- _xml:space: string (nullable = true)
 |    |-- timestamp: timestamp (nullable = true)
 |-- title: string (nullable = true)
```

## Question 2 

Runtime is 7 minutes and 49 seconds.
![alt text](./images/q2.png)

## Question 3

RUntime is 4 minutes and 11 seconds. It is an improvement over the previous setting. We can safely assume the extra instances are doing a lot of work. They are able to parrelize the work substantially.

![alt text](./images/q3.png)

## Question 4
The runtime is 4 minutes and 15 seconds. The improvement doesnt exist. We can therefore say that the block size for this doesnt make much difference. 
![alt text](./images/q4.png)


## Question 5

Orginally the program takes about 54 minutes and 42 seconds. If we do as the question asked, wethe new run time is 1 Hour and 47 minutes. This is a significant reduction. This can be due to the worker which is killed. Therefore it is not able to parrelize effective. It has only 2/3 of the resources. The process is completed successfully.  


![alt text](./images/q5.png)
![alt text](./images/q5_break.png)

## Question 6

If we change the replication factor, the runtime is 53 minutes and 23 seconds. This is not a improvement or reduction as the difference is really small.  The replication factor is not a factor in the runtime.

![alt text](./images/q6.png)

## Question 7

The block size doesnt have a effect on completion time. It is still the same at 53 Minutes and 41 seconds.

![alt text](./images/q7.png)

## Question 8

The final pagerank algorthms takes about 1 hour and 1 minute. 

![alt text](./images/pagerank.png)


## Question 9 

Total 1634800 elements with pagerank value of greater than 0.5