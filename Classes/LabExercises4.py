
# coding: utf-8

# # Apache Spark exercises for laboratory class 4
# 
# _Eduardo R. B. Marques, [Big Data and Cloud Computing](http://www.dcc.fc.up.pt/~edrdo/aulas/bdcc), DCC/FCUP_
# 
# __Aim__: introduction to the TF-IDF metric and its calculation using Spark
# 
# __Reference__: [Mining of Massive Data Sets, sec. 1.3.1](http://infolab.stanford.edu/~ullman/mmds/ch1.pdf), 
#     [Wikipedia page](https://en.wikipedia.org/wiki/Tf-idf)
# 
# 

# # The TF-IDF metric
# 
# ## Introduction 
# 
# __What good is the TF-IDF metric for?__
# 
# The TF-IDF metric  is a numerical statistic 
# that serves as a measure to reflect how important a word is to individual text documents 
# in the context of a corpus of documents. TF-IDF is widely used for text-based document search/recommendation. 
# 
# __Intuition__
# 
# Words that have a high frequency in a document may not be relevant to distinguish a document in a corpus of documents. For instance, the word "the" will very likely appear several times in an English documents (like "o" or "a" in Portuguese), and as such provides little information on any individual document. On the contrary, words that appear in only a few documents but with a relatively high frequency in some of them do convey significant information
# about the documents in which they appear. 
# 
# ## Definitions
# 
# There are several variations of the TF-IDF metric. The definitions here are as presented
# in [Mining of Massive Data Sets, sec. 1.3.1](http://infolab.stanford.edu/~ullman/mmds/ch1.pdf).
# 
# ### Term-frequency (TF)
# 
# Let $D$ be a set of documents, $d \in D$ be a document and $w \in d$ be a word.
# 
# Let $f(w,d)$ be the __number of occurrences__ of word $w$ in $d$.
# 
# The __term frequency (TF)__ of $w \in d$, reflecting the significance of $w$ in the (sole) context of $d$, is defined as:
# 
# $$
# {\rm TF}(w,d) = \frac{f(w,d)}{{\rm max} \{ f(w',d) : w' \in d \}}
# $$
# 
# i.e., the number of occurrences in $w$ in $d$ divided by the number of occurrences of the most common word in $d$.
# 
# Observe that for the most common word $w^* \in d$ we will have $f(d,w^*) = 1$.
# 
# ### Inverse document frequency (IDF)
# 
# Let ${\rm n}(w,D)$ be the __number of documents__ in which word $w$ appears:
# 
# $$
# {\rm n}(w,D) = | { d \in D : f(w,d) > 0 } | 
# $$
# 
# The __inverse document frequency (IDF)__ of word $w$ in $D$ is defined as: 
# 
# $$
# {\rm IDF}(w,D) = {\rm log}_{2}\left(\frac{|D|}{{\rm n}(w,D)}\right)
# $$
# 
# Observe that common words in all documents will have a low IDF value, in particular 
# ${\rm IDF}(w,D) = 0$ if ${\rm n}(w,D) = |D|$ ($w$ occurs in all documents). 
# 
# On the other hand, rare words will have a high IDF,
# in particular ${\rm IDF}(w,D) = log_2(|D|)$ if ${\rm n}(w,D) = 1$ (occurs in only 1 document).
# 
# ### TF-IDF
# 
# The __term frequency–inverse document frequency (TF-IDF)__ of a word $w$ in document $d$ is defined as 
# 
# $$
# {\rm TFIDF}(w,d) = {\rm TF}(w,d) \times {\rm IDF}(w,D)
# $$
# 
# Note that a high TF value if offset by a low IDF value in the TF-IDF, the case 
# when a word is common in many documents,  and vice-versa, the case when a word does not appear in many documents.
# 

# # PySpark code for TF-IDF calculation 
# 
# The following PySpark code uses the DataFrame API to provides a possible implementation for the TF-IDF calculation.
# 
# To be coherent with the above definitions for TF-IDF, it assumes that the source data frame `data` is composed of has columns `d` (for documents) and `w` (for  words). 

# In[1]:


def getTF(data, debug=False):
    f_wd = data       .groupBy('w','d')             .agg(F.count('w').alias('f_wd'))
    if debug:
        f_wd.orderBy('d','w').show()

    f_wd_max = f_wd             .groupBy('d')             .agg(F.max('f_wd').alias('f_wd_max'))
    if debug:
        f_wd_max.orderBy('d').show()
        
    TF = f_wd.join(f_wd_max, 'd')             .withColumn('TF', F.col('f_wd') / F.col('f_wd_max'))             .drop('f_wd','f_wd_max')
    return TF

def getIDF(data, debug=False):
    n_w_D = data           .groupBy('w')           .agg(F.countDistinct('d').alias('n_w_D'))
    if debug:
        n_w_D.orderBy('n_w_D',ascending=False).show()
        
    size_of_D = data.select('d').distinct().count()
    if debug:
        print("|D| = %d" % size_of_D)
    
    IDF = n_w_D            .withColumn('IDF', F.log2(size_of_D / F.col('n_w_D')))            .drop('n_w_D')
            
    return IDF
    
def getTF_IDF(data, debug=False):
    TF = getTF(data, debug)
    if debug:
        TF.orderBy(['d','TF'],ascending=[1,0]).show(TF.count())
    
    IDF = getIDF(data, debug)
    if debug:
        IDF.orderBy(['IDF','w'], ascending=[0,1]).show(IDF.count())

    TF_IDF = TF      .join(IDF,'w')      .withColumn('TF_IDF',F.col('TF') * F.col('IDF'))
        
    if debug:
        TF_IDF.orderBy(['d','TF_IDF','w'],ascending=[1,0,1]).show(TF_IDF.count())
    return TF_IDF


# # Testing with a small mock data set

# In[4]:


from pyspark.sql import functions as F
# ADD MORE EXAMPLES LATER IF YOU WISH
rdd = sc.parallelize(            [ (1, "how beautiful are the sun and the moon are they not yes they are"),
              (2, "how beautiful is that bird and that rose"),
              (3, "to be or not to be that is the question"),
              (4, "to bee or not to fly that is the question")
            ]);

print(rdd.collect())

data = sqlContext.createDataFrame(rdd, ['d', 'content'])           .withColumn('w', F.explode(F.split(F.col('content'), ' ')))           .drop('content')

data.show(data.count())

TF_IDF = getTF_IDF(data, debug=True)


# # Exercise 1. Making use of TF-IDF
# 
# Given TFIDF data and a word, return the document that has the highest TF-IDF score for the word.
# 

# In[38]:


def mostSignificantDoc(TF_IDF, word):
    return TF_IDF.filter(TF_IDF.w == word)            .orderBy('TF_IDF',ascending=False)            .collect()[0].d

for word in ['are', 'beautiful', 'bee', 'be']:
    print(word +  ' ' + str(mostSignificantDoc(TF_IDF,word)))


# # Exercise 2. Generalize for a list of words / list of documents
# 
# Given TFIDF data and a list of words, return a list of document that have the highest sums of TF-IDF scores for the given words i.e. documents $d$ that maximize
# 
# $$
# \sum_{w \:\in \:{\rm words}} {\rm TFIDF}(w,d)
# $$
# 

# In[41]:


from pyspark.sql import functions as F

def mostSignificantDocs(TF_IDF, wordList, maxDocuments=2,debug=False):
    search_data = spark.createDataFrame([(w,1) for w in wordList],['w','dummy']).drop('dummy')
    if debug:
        search_data.show()
        
    docs = TF_IDF                 .join(search_data,'w')
    
    if debug:
        docs.show()
        
    return docs.groupBy('d')                .agg(F.sum('TF_IDF').alias('score'))                .orderBy('score', ascending=False)

test = mostSignificantDocs(TF_IDF,['are','beautiful', 'bee'],debug=True)
test.show()

