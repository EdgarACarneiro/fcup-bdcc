
# coding: utf-8

# # Apache Spark exercises for laboratory class 3
# 
# _Eduardo R. B. Marques, [Big Data and Cloud Computing](http://www.dcc.fc.up.pt/~edrdo/aulas/bdcc), DCC/FCUP_
# 
# 

# In[1]:


# This block is required if you run the program using spark-submit
# or through Google DataProc or spark-submit
if __name__ == "__main__" :
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    spark = SparkSession        .builder        .appName("Lab3Exercises")        .master("local[*]")        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")


# ## Preliminaries 
# 
# ### Data from the Snap project
# 
# The [Snap project](https://snap.stanford.edu/index.html) at Stanford University collected and processed [several (anonymized) datasets](https://snap.stanford.edu/data/index.html#socnets) from social networks such as Facebook, Twitter, or Google+.
# 
# We will make use of Twitter data indicating how users relate
# to each other in terms of following. The data set at stake defines a directed graph with edges $a \rightarrow b$ with the following meaning:
# 
# $$a \rightarrow b \quad\equiv\quad a \: {\rm follows} \: b$$ 
# 
# The data set is a simple CSV file, with a short fragment shown next: 
# 
# ```
# user,follows
# 214328887,34428380
# 17116707,28465635
# 380580781,18996905
# 221036078,153460275
# ...
# ```
# 
# This means that user 214328887 follows user 34428380, user 17116707 follows user 28465635, etc.
# 
# 
# 
# ### What are we interested in ? 
# 
# - __Exercise 1. (warm-up exercise)__: Determining what are the most popular users, those with most followers, or the most "busy" ones, those that follow more users.
# 
# - __Exercise 2. Computing similarity between users__: we wish to identify the similarity between users based on what users they follow, or on what users follow them. For computing similarity we will make use of a metric called the __[Jaccard  index](https://en.wikipedia.org/wiki/Jaccard_index)__.
# 
# 
# 

# ## Getting started
# 
# Let us take take care of loading the data set, stored in the BDCC bucket.

# In[2]:


dataSetFile = 'twitter_combined-10000.txt'
gcsFile='gs://bdcc1819/datasets/snap_twitter/combined/' + dataSetFile

data = spark      .read      .csv(gcsFile, inferSchema=True, header=True)      .cache() # Cache data for improved performance
    
print('== FILE ==')
print(gcsFile)

print('== Schema ==')
data.printSchema()

print('== A few entries ==')
data.show(5)


# # Exercise 1. 
# 
# ### 1.1. Calculate the top 10 users with most followers.
# 
# 

# In[54]:


from pyspark.sql import functions as F


most_followers = data.groupBy('follows')                        .agg(F.count('user').alias('followers'))                        .sort('followers', ascending=False)                        .withColumnRenamed('follows', 'user')

most_followers.show(10)


# ### 1.2. Calculate the top 10 users that follow more users.

# In[58]:


most_follows = data.groupBy('user')                        .agg(F.count('follows').alias('following'))                        .sort('following', ascending=False)
most_follows.show(10)


# # Exercise 2
# 
# ## The Jaccard index
# 
# Given sets $A$ and $B$ the __Jaccard index__ of sets $A, B \neq \emptyset$ is given by
# 
# $$
# JI(A,B) = \frac{|\quad A \cap B \quad|}{|\quad A \cup B \quad|}
# $$
# that is, the number of elements in $A \cap B$ divided by the number of elements in $A \cup B$.
# 
# Observe that $JI(A,B) \in [0,1]$, meaning that:
# 
#   - a value close to $0$ indicates low similarity;
#   - a value close to $1$ indicates high similarity;
# 
# and that we may have the following extreme cases:
#    
#    -  $JI(A,B) = 1$ when $A=B$
#    -  $JI(A,B) = 0$ when $A\cap B = \emptyset$
# 
# <img src="A_int_B.png" alt="intersection" width="191"/> <img src="A_union_B.png" alt="union" width="191"/>
# 
# 

# ## 2.1. Similarity based on followed users (solution given)
# 
# Calculate the similarity of users based on followed users.
# 
# ### Algorithm
# 
# A possible algorithm involves the following steps:
# 
# __Step 1__ 
# 
# Calculate the set of followed users  per user first, i.e., 
# a data frame with rows
# 
#    $$
#    (u, f(u)) \quad {\rm where}\quad f(u) = \{ u' : u \:{\rm follows}\: u' \})
#    $$
# 
# 
# __Step 2__ 
# 
# We can combine the above pairs through a filtered
# cross-join (cartesian product):
# 
#    $$
#    (u_1, f(u_1), u_2, f(u_2)) \quad {\rm where}\quad u_1 < u_2
#    $$
#    
#    The  $u_1 < u_2$ filter avoids subsequent duplicate
#    calculations (note that $JI(A,B) = JI(B,A)$).
# 
# __Step 3__
# 
# We can now compute a relation with columns:
# 
#    $$ 
#    (u_1, u_2, I(u_1,u_2), U(u_1,u_2))
#    $$
#    
#    where
#    
#    $$
#    I(u_1,u_2) = f(u_1) \cap f(u_2)\quad U(u_1,u_2) = f(u_1) \cup f(u_2)
#    $$
# 
# __Step 4__
# 
# The Jaccard index can now be calculated per pair of users:
# 
#    
#    $$ 
#    (u_1, u_2, JI(u_1,u_2))
#    $$
#    
#    where
#    
#    $$
#    JI(u_1,u_2) = \frac{|\quad I(u_1,u_2)\quad|}{|\quad U(u_1,u_2)\quad|}
#    $$
#    
# 
# 
# ### The PySpark code for the algorithm 

# In[59]:


from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType,IntegerType

# Define F.array_intersect if not defined (Spark version < 2.4)
if not hasattr(F,'array_intersect'):
  F.array_intersect = spark.udf    .register('array_intersect', 
       lambda x,y: list(set(x) & set(y)), ArrayType(IntegerType()))

# Define F.array_union if not defined (Spark version < 2.4)
if not hasattr(F,'array_union'):
  F.array_union = spark.udf    .register('array_union', 
       lambda x,y: list(set(x) | set(y)), ArrayType(IntegerType()))

def similaryBasedOnFollowedUsers(data,minFollows=20,debug=False):
   
  # We start by renaming the user column in line with the notation 
  # above.
  data = data.withColumnRenamed('user','u1')

  # ==== Step 1 ====
  u1_fu1 = data       .groupBy('u1')       .agg(F.collect_set(data.follows)             .alias('fu1')           )       .filter(F.size('fu1') >= minFollows)

  if (debug):
    print('>> Step 1 :: u1 f(u1) <<')
    u1_fu1.show()

  # ==== Step 2 ==== 
  # First create a "dual" of data by renaming columns.
  # This will help the subsequent join.
  u2_fu2 = u1_fu1        .withColumnRenamed('u1', 'u2')        .withColumnRenamed('fu1', 'fu2')
    
  prod = u1_fu1    .crossJoin(u2_fu2)    .filter(u1_fu1.u1 < u2_fu2.u2)

  if (debug):
    print('>> Step 2 :: u1 f(u1) u2 f(u2) <<')
    prod.show()

  # ==== Step 3 ==== 
  prod2 = prod   .withColumn('I',       F.array_intersect(prod.fu1,                         prod.fu2))   .withColumn('U',       F.array_union(prod.fu1,                     prod.fu2))   .drop('fu1','fu2')

  if (debug):
    print('>> Step 3 :: u1 u2 I(u1,u2) U(u1,u2) <<')
    #prod2.orderBy('I',ascending=False).show()
    prod2.show()
    
  # ==== Step 4 ====
  result = prod2   .withColumn('JI', F.size('I') / F.size('U'))   .drop('I','U')

  if (debug):
    print('>> Step 4 :: u1 u2 J(u1,u2) <<')
    result.show()
  return result



# ### Testing with a mock data set
# 
# We also consider a mock data set to make the Jaccard Index easier to understand than the longer Twitter data set (see below).
# 
# ```
# | User | Follows      | Followers      |
# | ---- | ------------ | -------------- |
# | 1    | { 2, 3, 4 }  | { 2 }          |    
# | 2    | {1, 3, 4, 5} | { 1, 4 }       |
# | 3    | {4, 5}       | { 1, 2, 4 }    |
# | 4    | {2, 3, 5}    | { 1, 2, 3, 5 } |
# | 5    | {4}          | { 2, 3, 4 }    |
# ```

# In[60]:


mockRDD = sc.parallelize(            [ (1,2), (1,3), (1,4),
             (2,1), (2,3), (2,4), (2,5),
             (3,4), (3,5), 
             (4,2), (4,3), (4,5),
             (5,4) ])
mockData = sqlContext.createDataFrame(mockRDD, ['user', 'follows']).cache()
mockData.show()
similaryBasedOnFollowedUsers(mockData,minFollows=0,debug=True)


# ### Testing with the Snap data set

# In[61]:


similaryBasedOnFollowedUsers(data,minFollows=20,debug=False)  .orderBy('JI',ascending=False).show()


# ## 2.2. Similarity based on followers
# 
# Calculate the similarity of users based on __followers__ rather than followed users.
# 
# An adaption of the code above can be used to solve this problem.
# 

# In[67]:


def similaryBasedOnFollowers(data, minFollowers=20,debug=False):
   
  # We start by renaming the user column in line with the notation 
  # above.
  data = data.withColumnRenamed('follows','u1')

  # ==== Step 1 ====
  u1_fu1 = data       .groupBy('u1')       .agg(F.collect_set(data.user)             .alias('fu1')           )       .filter(F.size('fu1') >= minFollowers)

  if (debug):
    print('>> Step 1 :: u1 f(u1) <<')
    u1_fu1.show()

  # ==== Step 2 ==== 
  # First create a "dual" of data by renaming columns.
  # This will help the subsequent join.
  u2_fu2 = u1_fu1        .withColumnRenamed('u1', 'u2')        .withColumnRenamed('fu1', 'fu2')
    
  prod = u1_fu1    .crossJoin(u2_fu2)    .filter(u1_fu1.u1 < u2_fu2.u2)

  if (debug):
    print('>> Step 2 :: u1 f(u1) u2 f(u2) <<')
    prod.show()

  # ==== Step 3 ==== 
  prod2 = prod   .withColumn('I',       F.array_intersect(prod.fu1,                         prod.fu2))   .withColumn('U',       F.array_union(prod.fu1,                     prod.fu2))   .drop('fu1','fu2')

  if (debug):
    print('>> Step 3 :: u1 u2 I(u1,u2) U(u1,u2) <<')
    #prod2.orderBy('I',ascending=False).show()
    prod2.show()
    
  # ==== Step 4 ====
  result = prod2   .withColumn('JI', F.size('I') / F.size('U'))   .drop('I','U')

  if (debug):
    print('>> Step 4 :: u1 u2 J(u1,u2) <<')
    result.show()
  return result

similaryBasedOnFollowers(data,minFollowers=20,debug=False)  .orderBy('JI',ascending=False).show()


# ## 2.3. Recommending users based on similarity
# 
# Devise algorithm for recommending users according 
# to simple strategies.
# 
# Start by considering the context of a single user $u_1$, then try to generalize.
# 
# __Follows-based similarity__
# 
# - For $u_1$ pick a followed user $u_2$ that maximizes $J(u_1,u_2)$.
# - Recommend an user $u_3$ that is followed by $u_2$ but not yet followed
# by $u_1$.
# 
# __Follower-based similarity__
# 
# - For an user $u_1$ pick a followed user $u_2$ that has the most
# followers.
# - Recommend an user to follow $u_3$ that maximizes $J(u_2,u_3)$.
# 
# 

# In[ ]:


# Fazer um join com a tabela de user follows, para saber que users o u2 segue, 
# desses escolher o que o u3 não chegue e depois selecionar o de maior JI
def follows_based_similarity(data, u1):
    simJI = similaryBasedOnFollowedUsers(data).orderBy('JI',ascending=False)
    u2 = simJI.filter(simJI.u1 == u1).limit(100)
    u2_follows = data.join(u2, data.user == u2.u2, 'left_semi')
    u1_not_follows = data.subtract(data.filter(data.follows == u1))
    u2_follows.join(u1_not_follows, u2_follows.follows == u1_not_follows.user, 'cross').show()
    
# Test
follows_based_similarity(data, 259842341)

