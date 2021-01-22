#!/usr/bin/env python
# coding: utf-8

# # Exercise 2: Advanced Analytics NLP

# In[1]:


get_ipython().system('pip install spark-nlp==2.4.5')


# In[2]:


import pandas as pd
pd.set_option('max_colwidth', 800)


# # Create a spark context that includes a 3rd party jar for NLP

# In[3]:


#jarPath = "spark-nlp-assembly-1.7.3.jar"

from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars.packages","com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.5").getOrCreate()
spark


# # Read multiple files in a dir as one Dataframe

# In[4]:


dataPath = "./data/reddit/*.json"
df = spark.read.json(dataPath)
print(df.count())
df.printSchema()


# In[5]:


df.select("data.author","data.title").limit(5).toPandas()


# # Deal with Struct type to query subfields 

# In[6]:


title = "data.title"
author = "data.author"

dfAuthorTitle = df.select(title,author)
dfAuthorTitle.limit(5).toPandas()


# # Try to implement the equivalent of flatMap in dataframes

# In[7]:


import pyspark.sql.functions as F

dfWordCount = dfAuthorTitle.select(F.explode(F.split("title", "\\s+")).alias('word')).groupBy('word').count().orderBy(F.desc('count')).limit(100).toPandas()


# # Use an NLP libary to do Part-of-Speech Tagging

# In[8]:


import sparknlp


# In[9]:


from sparknlp.pretrained import PretrainedPipeline


# In[10]:


pipeline = PretrainedPipeline('explain_document_ml',lang='en')


# In[11]:


dfAnnotated = pipeline.annotate(dfAuthorTitle,'title')


# In[12]:


dfAnnotated.printSchema()


# ## Deal with Map type to query subfields

# In[14]:


dfPos = dfAnnotated.select("text","pos.metadata","pos.result")
dfPos.limit(5).toPandas()


# In[15]:


dfPos=dfAnnotated.select(F.explode("pos").alias("pos"))
dfPos.printSchema()
dfPos.limit(5).toPandas()


# ## Keep only proper nouns NNP or NNPS

# In[24]:


from pyspark.sql.functions import array_contains, col


# In[37]:


#nnpFilter = "pos.result = 'NNP' or pos.result = 'NNPS' "
dfNNP = dfPos.filter((col("pos.result") == "NNP") | (col("pos.result") == 'NNPS'))
dfNNP.filter(col("pos.result") != 'NNP').limit(20).toPandas()


# ## Extract columns form a map in a col

# In[38]:


dfNNP.printSchema()


# In[40]:


dfWordTag = dfNNP.selectExpr("pos.metadata['word'] as word", "pos.result as tag")
dfWordTag.limit(10).toPandas()


# In[ ]:


from pyspark.sql.functions import desc
dfWordTag.groupBy("word").count().orderBy(desc("count")).show()


# In[ ]:




