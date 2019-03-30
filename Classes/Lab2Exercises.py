
# coding: utf-8

# # Apache Spark exercises for laboratory class 2
# 
# _Eduardo R. B. Marques, [Big Data and Cloud Computing](http://www.dcc.fc.up.pt/~edrdo/aulas/bdcc), DCC/FCUP_
# 
# ## MovieLens data set for this class
# 
# For some exercises will  use a small dataset derived from other GroupLens data sets. 
# The data set is located at 
# 
# ```
# gs://bdcc1819/datasets/MovieLens/lab2
# ```
# 
# and contains data for 535 movies, 5657 ratings, and 184 tags.
# 
# For faster read performance you should __first copy the data set into the Spark master machine using__
# ```
# gsutil cp -r gs://bdcc1819/datasets/MovieLens/lab2 /tmp
# ```
# This command must be __executed on the Spark cluster machine__.
# 
# The code below for loading the data, assumes that you have done this and, thus, the data set is available in the local file system of the Spark instance.
# 
# ## Other MovieLens data sets
# 
# Two other larger MovieLens data sets are available at the BDCC bucket if you would like to experiment with them later:
# 
# ```
# gs://bdcc1819/datasets/MovieLens/ml-latest-small
# ```
# has ~10 000 movies, ~100 000 ratings, and ~3500 tags,
# and 
# 
# ```
# gs://bdcc1819/datasets/MovieLens/ml-20m
# ```
# 
# is a "big" data set with ~30 000 movies, ~20 million ratings, ~0.5 million tags. 
# 
# ## In case Jupyter notebook becomes unstable / unresponsive ...
# 
# If you start to get errors complaining about not being able to connect to the
# Jupyter kernel:
# 
# 1. It is good to have only a notebook running. From the `Home` page you can shutdown
# running notebooks.
# 
# 2. In the notebook access `File > Close And Halt` then try to access the notebook again from the `Home` page.
# 
# 3. As a last resort, stop and restart the cluster machine.
# 
# 
# 
# 

# In[1]:


if __name__ == "__main__" :
    # This block is required to run the program from the command line
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    spark = SparkSession        .builder        .appName("Lab2Exercises")        .master("local[*]")        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

def readCSV(file): 
    print('Reading ' + file)
    df = spark.read.csv(file, inferSchema=True, header=True)
    print('Data frame read - it has %d partitions.' % df.rdd.getNumPartitions())
    df.printSchema()
    df.show(3)
    return df

def loadMovieLensData(path):
    global movies, ratings, tags
    movies  = readCSV(path +'/movies.csv').cache()
    ratings = readCSV(path +'/ratings.csv').cache()
    tags = readCSV(path +'/tags.csv').cache()


#loadMovieLensData('gs://bdcc1819/datasets/MovieLens/lab2')
loadMovieLensData('file:///tmp/lab2')
print('#movies = %d' % movies.count())
print('#ratings = %d' % ratings.count())
print('#tags = %d' % tags.count())


# ## 1. Working with RDDs
# 
# For a data frame `df` you can obtain the underlying RDD 
# through the 
# [rdd](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.rdd) 
#       attribute, i.e.,  `df.rdd`. 
#       
# The obtained RDD is composed of [Row](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Row) objects.
# You may access the values of row objects by name.
# 
# This is examplified below for the `tags` data frame.
# 
# 
#                                                                           

# In[2]:


tagsRDD = tags.rdd
firstTwoRows = tagsRDD.take(2)
print(firstTwoRows[0])
print(firstTwoRows[1])
print(firstTwoRows[0].tag)
print(firstTwoRows[1].movieId)


# ### 1.1. Aggregate count per movie tag
# 
# Using RDD methods [map](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.map), [reduceByKey](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.reduceByKey), and [sortByKey](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.sortByKey)
# derive a `(tag, tagCount)` key-value RDD from `tagsRDD` that 
# associates each tag to the total number of ocurrences of the tag,
# and that is ordered by tag name.

# In[22]:


tagCountRDD = tagsRDD.map(lambda row: (row.tag, 1))                     .reduceByKey(lambda acc, val: acc + val)                     .sortByKey(True, 3, keyfunc=lambda k: k.lower())

tagCountRDD.collect()


# ### 1.2. Aggregate movie sets by tag
# 
# Now derive a `(tag,movieIdSet)` RDD that 
# associates each tag to the set of movie ids the tag has been used with.
# Recall that in Python:
# 
# - `{ elem }` is a singleton set containing `elem`
# - `a | b` calculates the union of sets `a` and `b`

# In[23]:


tagMovieSetRDD = tagsRDD.map(lambda row: (row.tag, {row.movieId}))                        .reduceByKey(lambda acc, val: acc | val)
tagMovieSetRDD.collect()


# 
# ### 1.3. Multiple aggregations per tag
# 
# Compute in a single data flow both the tag counts and movie sets as before, and also the maximum timestamp for each tag, that is, derive a `(tag, (count, movieIdSet, maxTimestamp))` RDD.
# 

# In[29]:


print('RDD for 1.3')
tagCombRDD = tagsRDD.map(lambda row: (row.tag, (1, {row.movieId}, row.timestamp)))                    .reduceByKey(lambda acc, val: (acc[0] + val[0], acc[1] | val[1], max(acc[2], val[2])))
tagCombRDD.collect()


# ##  2. Data Frame API
# 
# ### Code to obtain top-rated movies
# 
# Below is some code similar to what we've  seen before for obtaining top-rated movies with at least a certain number of ratings. The code may be used as a guideline for solving the exercises in this section.

# In[30]:


import time
from pyspark.sql import functions as F

def getTopMovies(dfMovies, dfRatings, minNumRatings, n=None):
    t = time.time()
    aggRatings = dfRatings.groupBy('movieId')                         .agg(F.count('rating').alias('num'),                              F.avg('rating').alias('avg'),                              F.min('rating').alias('min'),                              F.max('rating').alias('max'))
    sortedRatings = aggRatings.filter(aggRatings.num > minNumRatings)                            .join(dfMovies, 'movieId')                            .select('movieId',dfMovies.title, aggRatings.num, aggRatings.avg)                            .sort(aggRatings.avg.desc())
    if n != None :
      sortedRatings = sortedRatings.limit(n)
    return sortedRatings

# Define data frame
minRatings = 10
n = 20
topMovies = getTopMovies(movies, ratings, n)

print('Getting top %d movies with at least %d ratings ...' % (n, minRatings))
t = time.time()
topMovies.show(n)
t = time.time() - t
print('Done in %.2f seconds.' % t)


# 
# 
# ### 2.1. Aggregation functions
# 
# Use the `tags` data frame directly and the associated [DataFrame API](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame) to compute the same data set as in 1.3 (or, alternatively 1.1 & 1.2 separately before considering 1.3). Part of the code to obtain top-rated movies (below in the notebook)
# may be used as guideline to solve this exercise.
# 
# You should need to make use of [groupBy](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.groupBy), [agg](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData.agg), and [orderBysort](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.orderBy) (or sort) methods. 
# 
# As aggregation functions you should need to make use of [pyspark.sql.functions](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions): [count](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.count),  [collect_set](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.collect_set), and [max](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.sum).

# In[41]:


from pyspark.sql import functions as F

aggDF = tags.groupBy('tag')            .agg(F.count('tag'),                 F.collect_set('movieId'),                 F.max('timestamp'))

aggDF.show(aggDF.count())


# ### 2.2. Christmas movies by title
# 
# Derive a data frame for movies whose title contains the word 'Christmas', then use 'topRatings' to see the top ratings for them.
# 
# You  should be able to accomplish this using the [DataFrame.filter](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.filter) method in association with [Column.contains](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.contains).
# 
# 

# In[53]:


cmByTitle = movies.filter(movies.title.contains('Christmas'))
cmByTitle.show()

# Define data frame
minRatings = 1
n = 20
topMovies = getTopMovies(cmByTitle, ratings, n)

print('Getting top %d movies with at least %d ratings ...' % (n, minRatings))
t = time.time()
topMovies.show(n)
t = time.time() - t
print('Done in %.2f seconds.' % t)


# ### 2.3. Christmas movies by tag
# 
# It may well be that not all Christmas movies have the word 'Christmas' in their title!
# What about searching for movies tagged with the 'Christmas' or 'christmas' tags?
# 
# 1. First, derive a data frame from `tags` such that the `tag` field is either 'Christmas' or 'christmas'. Note: for this purpose you can use the syntax `condition_1 | condition_2` in association to [DataFrame.filter](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.filter).
# 
# 2. You should then can perform a `'left-semi'` [join](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.join)  between the `movies` data frame and the data frame obtained in the previous step. A left-semi join differs from the standard column-join in that it will only select columns from `movies` (which is what we want).
# 
# 

# In[56]:


ctags = tags.filter(tags.tag.contains('Christmas') | tags.tag.contains('christmas'))
ctags.show()
cmByTag = movies.join(ctags, ctags.movieId == movies.movieId, 'left_semi')
cmByTag.show()


# ### 2.4. Christmas movies by title or tag
# 
# Derive a data frame that is the union of `cmByTitle` and `cmByTag`. You can make use of the [union](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.union) followed by [distinct](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.distinct) to avoid repeated movies.

# In[61]:


cmByTitleOrTag = cmByTitle.union(cmByTag).distinct()
cmByTitleOrTag.show()


# ## 3. Running from the command line
# 
# 1. Download this notebook as a Python file using `File > Download As > Python`
# 
# 2. Copy the `submitJob.sh` script from `gs://bdcc1819/sparkClusterScripts`
#    
#    ```
#    gsutil cp gs://bdcc1819/sparkClusterScripts/submitJob.sh .
#    chmod +x submitJob.sh
#    ```
# 
# 3. Run the PySpark program by using the script as follows in your local machine:
#    ```
#    ./submitJob.sh clusterName Lab2Exercises.py
#    ```
