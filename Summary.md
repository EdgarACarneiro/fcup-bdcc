HDFS architecture# Big Data and Cloud Computing Summary

## Introduction

> The “data deluge” problem

* There is a deluge of big data, collected and queried “every second” from “everywhere”
* Raw” big data is not self-explanatory: we must __mine value__ from it, to seek for emerging patterns and correlations, validate scientific theories,

>  Big Data – the NIST definition

"Big Data consists of extensive datasets primarily in the characteristics of volume, variety, velocity, and/or variability that require a scalable architecture for efficient storage, manipulation, and analysis."

>  The Vs of Big Data

* __Volume__: has a high volume;
* __Velocity__: is generated / needs to be processed at a high rate;
* __Variety__: can originate from multiple sources / be of multiple types;
* __Variability__: exhibits variability in respect to the other Vs or other factors;

Two other importat Vs may also be considered:

* __Veracity__: the need to infer (measurable, principled) veracity;
* __Value__: the inherent value of data / its potencial to generate value.

>  Cloud computing – the NIST definition

"Cloud computing is a model for enabling ubiquitous, convenient, on-demand network access to a __shared pool of configurable computing resources__ (e.g., networks, servers, storage, applications, and services) that can be rapidly provisioned and released with minimal management effort or service provider interaction."

* __How does it provide?__ On-demand scalable services available through the network, in automated and rapid manner.

## Cloud Computing – Architecture and Services

> CC essential characteristics

* __On-demand self-service__: A consumer can provision computing resources without human intervention.
* __Broad network access__: The cloud services are available over the network through standard means (e.g. Internet connection).
* __Resource pooling__: Resources are pooled by the cloud provider to serve multiple consumers using a multi-tenant model.
* __Rapid elasticity__: The amount of computing resources can be adjusted/scaled dynamically at the consumer’s request or automatically in some configurable manner by the consumer.
* __Measured service__: Resource usage can be tracked and measured precisely by both the cloud provider and consumer, for instance to allow for a clear billing analysis.

> Service Models

* __Software-as-a-Service (SaaS)__: Customers get access to software running on the cloud (e.g. Google Apps, Office 365).
* __Platform-as-a-service (PaaS)__: Customers get a platform that supports the development and/or deployment of cloud applications (e.g. database engines, Hadoop clusters, Google App Engine).
* __Infrastructure-as-a-Service (IaaS)__: Customers get actual infrastructure in the form of computing resources (e.g., for processing, storage, ornetworking purposes).

Service Layer: SaaS > PaaS > IaaS

> Deployment models

* A __public cloud__ is open for use to the general public by a cloud provider.
* A __private cloud__ is used exclusively in the scope of a single organization.
* A __community cloud__ is shared by several organizations.
* A __hybrid cloud__ results from the combination of two or more clouds that exist separately but are interoperable in some way.

> Service orchestration

__Resourceabstraction and control layer__: mediates the use of physical resources by services (with techniques such as virtualization). Is usually exposes __virtual resources__ on top of the __physical resource layer__.

> Virtualization

A __virtual resource__ provides the illusion of a real resource (computer, storage, network) through diverse context-specific techniques.

> Elasticity

__Elasticity__: the ability of a cloud service to grow or contract provisioned resources in line with service demand.

> Storage service – notion of bucket

A bucket is identified by a unique (global-level) __name__, and holds a set of (__key-value__) __objects__. Each object in a bucket is identified by a
(bucket-level) unique __key__ (a name), and with an associated __value__ in the form of arbitrary binary data, with size ranging from a few bytes to terabytes.

A bucket may be __available/replicatedy__ globally or across regions, and has configurable access permissions

## Introduction to MapReduce

__MapReduce__ is a programming model for automatic parallelisation of computing tasks across large-scale clusters of machines.

The purpose of MapReduce is to define a simple programming model such that:
* programs specify computation in terms of __map__ and __reduce__ functions that do not require any knowledge of parallel computing, and at the same time are __inherently parallelizable__.
* a runtime system handles the __automatic parallelisation of computation__, dealing with aspects such as data/work distribution, inter-machine communication, or machine failures.

Map/reduce in `python3`:
```python
map_l = map(lambda x: x + 1, L1))
red_l = reduce(lambda x,y : x + y, L1)
```

> Why are map & reduce convenient abstractions for parallel programming?

__Functions have no state__. This allows the map and reduce operations to be executed in flexible order and in parallel. They also require no knowledge of parallel programming.

> Google MapReduce

.... (Techinicalities over Haddoop, Apache Spark). Apache Spark > Haddoop yarn.

## Introduction to PySpark - RDDs

__Resilient Distributed Datasets (RDDs)__ are "immutable, partitioned collection of elements that can be operated on in parallel".

__Transformations__ are __lazily evaluated__, such as mapping and reducing. However, __actions__ force the evaluation of the RDD. An example of an action used to collect results is:
```python
rdd.collect()
```

Example of mapping and reducing an RDD:
```python
tagCountRDD = tagsRDD.map(lambda row: (row.tag, 1))\
                        .reduceByKey(lambda acc, val: acc + val)
```

...

## Introduction to Spark data frames

__Data frames__ are similar to RDDs, in fact they are supported internally by RDDs, but data is organized as named columns that let you process data more easily.

* `printSchema()`: to print the Dataframe schema.
* `take(num)`:  yields the first _num_ elements in the DataFrame. 

Notice that actions such as take force __lazy__ transformations to take place.

* `show()`: get glimpse of contents.
* `filter(condition)`: filter rows satisfying the given condition.
* `sort(row_to_sort)`: sort the dataframe by the given row.

* __Narrows transformations__, such as _filter_, are a one-to-one transformation and therefore do not require Spark to move around data between workers.
* __Wide transformations__, such as _sort_, force data to be collected from many input partitions to derive new output partitions (many-to-many).

`Filter` and `sort` example:
```python
ratingsForToyStory = ratings\
    .filter(ratings.movieId == 1)\
    .sort(ratings.rating)
```

> Aggregations

Example of aggregation on dataframes, using the `groupBy` transformation first:
```python3
ratings.groupBy('movieId')\
                    .agg(F.count('rating').alias('num'),\
                         F.avg('rating').alias('avg'),\
                         F.min('rating').alias('min'),\
                         F.max('rating').alias('max'))
```

> Joins

We can also join to dataframes using the operator `join`. Notice, however that there must be on column with the same name on both DFs. Example:

```python
aggRatings.join(movies, 'movieId')\
          .select(movies.title, aggRatings.num, aggRatings.avg)\
```

As we can see above, the operator `select` serves to select the columns of interest.

Additionally, there is also the transformation `distinct` serving to remove doubles in a DF. Example:
```python
tags.filter(tags.tag == 'Alfred Hitchcock')\
    .select('movieId')\
    .distinct()
```

## Data partitioning and persistence in Spark
Use `DF.cache()` to persist data and `DF.unpersist(True)` to unpersist.

To obtain an RDD from a DF use:
```python
rdd = df.rdd
```

...

...

...

## HDFS and YARN

> The Hadoop ecosystem

Apache Spark runs in articulation with the Hadoop “ecosystem” along with many other frameworks. The backbone core components of Hadoop are: __HDFS (Hadoop Distributed File System)__: a file system for storing very large files in computer clusters; and __YARN (Yet Another Resource Negotiator)__: a system for running computation in computer clusters.

> HDFS design goals

HDFS, originally inspired by the Google File System (GFS), is a file system designed to store __very large files__ across __distributed__ machines in a __large cluster__ with __streaming data access patterns__:
* __streaming data access pattern__ → files are typically __written once__ or in append mode, and __read many times__ subsequently.

> HDFS is not good for
* Multiple writers and/or random file access operations
* Low-latency operations
* Lots of small files (HDFS is not designed with small files in mind).

> HDFS architecture

HDFS clusters are composed of __namenodes__ and __datanodes__:
* __Namenodes__ manage the file system and its meta-data;
* __Datanodes__ provide actual storage;

> HDFS files
A HDFS file provides the abstraction of a single file, but is in fact divided into __blocks__ ( ~64/ 128 MB). A __block__ is the elementary unit for read / write operations by client applications, and each is __stored and replicated independently__ in different machines. The host file system in datanodes stores blocks as regular files.

Advantages of splitting file into several replicated blocks:
* __Support for “really big files”__
* __Fault tolerance / high availability__: if a block becomes unavailable from a datanode, a replica can be read from another datanode.
* __Data locality in integration with MapReduce__: Hadoop MapReduce takes advantage of the block separation to move __computation near to where data is located__, rather than the other way round that would be quite more costly in terms of network communication/operation time. __Moving computation is cheaper than moving data__.

![HDFS file Reads](https://i.imgur.com/kvUrYL6.png)
![HDFS file Writes](https://i.imgur.com/S4lCQ0V.png)

> YARN

YARN is a system for running computations in computer clusters. Its architecture resembles that of Spark (not coincidentally). The __Resource manager (RM)__ manages and mediates access to resources provided at each (worker) machine through __node managers (NMs)__.

> YARN Architecture

Clients start by asking the RM to run an __application master process (AMP)__. The RM finds a suitable NM to handle its execution. A __container__ is responsible for executing an application-specific process, including the AMP, with a constrained budget of resources
(CPU, memory, ... ).

Within its container, the AMP may run computation and terminate, or require more containers to run other application-specific processes. __Analogy to Spark__: AMP = Spark driver process; other processes = Spark executors.

YARN is agnostic to the way client/AMP/other processes of an application communicate.

> YARN Scheduler

__YARN scheduler__ governs the dynamic allocation of resources to applications. Responsible for applying a scheduling policy to define when and what resources are provided to application processes.

Three type of YARN schedulers:
* __First-In First-Out (FIFO) scheduler__: applications are placed in a queue and run in submission order (first in, first out), each execution occupying the entire cluster.
    * Unsuitable for shared clusters! A long-running application will keep other more modest applications waiting.
* __Capacity Scheduler__: multiple queues are employed, each with a certain number of fixed resources, so that the cluster resources may be shared by several running applications.
    * Suitable for shared clusters. Resources may be under-utilized though
* __Fair Scheduler__: the resources are dynamically balanced/allocated between/to applications, without the need to set a fixed capacity. _(best one)_
    * Dynamic (i.e. preemptive) resource allocation ensures that resources are not under-utilized.

> Google Dataflow vs Dataproc
* __Google Dataflow__: place to run Apache Beam based jobs, without the need to address the technicalities
    of running jobs on a cluster (such as work balancing and scaling), since this is automatically done for you.
    With Google Dataflow you focus on the logical computation rather than how the runner works.
    Additionally, Dataflow provides base templates that simplify common tasks. Beams has the downside of
    only supporting Python 2.7.
    
* __Dataproc__: provides you with a Hadoop Cluster and access to Apache Hadoop / Spark ecosystem.
    Indicated when the manual provisioning of clusters is necessary (as seen before, GD does it automatically)
    OR when there are dependencies to tools belonging to the aforementioned ecosystem.
    

> Apache BEAM

* __Pipeline__: encapsulates the workflow of your entire data
                processing tasks from start to finish.
 
* __PCollection__: distributed data set that your Beam
                   pipeline operates on.
                
* __PTransform__: represents a data processing operation, or a
                  step, in your pipeline
* __ParDo__: for generic parallel processing, considers each element in the input
                                              PCollection, performs some processing function (your user
                                              code) on that element, and emits zero, one, or multiple
                                              elements to an output PCollection
* __DoFn__:  applies your logic in each element in the input
            PCollection and lets you populate the elements of an
            output PCollection 

> TensorFlow

* __TensorFlow Transformations__: are great for preprocessing input data for TensorFlow, including creating features
that require a full pass over the training dataset.

* __Tensroflow Estimators__: base themselves on the Estimator class, which wraps a model which is specified by a
model function, which, given inputs and a number of other parameters, returns the operations necessary to
perform training, evaluation, or predictions.

> Map Reduce vs Parallel DBMS

>Advantages of map-reduce over parallel and distributed databases:
* __Inferior data loading time__ since there isn't any kind of preprocessing of the data, whilst there is on
parallel and distributed databases;
* __More fault-tolerant__ then its counterpart
* __Open-source implementations__ while the major set of parallel and distributed databases are very
    expensive;
* __No schema is enforced__ as the data does not need to follow any kind of schema which can be good in
  cases of semi-structured data.
* __Allows for more complex data interrogation__ while in parallel and distributed databases the SQL
    language can be a restriction in the type of queries that can be made.
* __Excel at complex analytics and ETL (Extract Transform Load) tasks__

>Advantages of map-reduce over parallel and distributed databases:
* __Use of indexes__ greatly improving query time when compared to the map-reduce approach;
* __Works with compressed data__, making the nodes access to data faster;
* __Automatic optimizations in task distribution__ - the system would distrbute tasks in such way that the
    flux of data between nodes is minimized;
* __Schema is enforced__ which pushes programmers to a higher, more-productive level of abstraction.
* __Faster data interrogation__, complex queries in the map-reduce approach are simplified by the usage of
    the SQL language
    
>It's suggested that parallel DBMS should be chosen
above MapReduce when:
* The data is structured and will continue to be so for the foreseeable future.
* The data set is large and is expected a large amount of complex querying.

> Multi-Relational Data

Most machine learning methods build models based on a
 2-dimensional table, where, usually, rows are instances and
 columns are variables
 
Relations
may exist among instances, not only among
variables

Data may need to be preprocessed:  often big/full join of
multiple tables but that:
* may introduce redundancy or bias

* consumes space

Ideal systems must deal with:
* uncertainty;
* multiple relations;
* multiple modalities;
* streamed data;
* explain the model;
* consuming a minimum number of resources.


> Explanatory Models:
* Rules (multi-relational if the representation is in first-order logic)
* Decision Trees (not multi-relational)
* Bayesian Networks (not multi-relational)

