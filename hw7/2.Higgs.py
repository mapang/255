# -*- coding: utf-8 -*-
#Name: Michelle Pang
#Email: mapang@eng.ucsd.edu
#PID: A10660264

from pyspark import SparkContext
sc = SparkContext()




# In[3]:

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel

from pyspark.mllib.util import MLUtils

# In[6]:

# Read the file into an RDD  #THIS IS WHAT YOU REPLACE WHEN TURNING INTO PYOBLT
# If doing this on a real cluster, you need the file to be available on all nodes, ideally in HDFS.
path= '/HIGGS/HIGGS.csv'  #'higgs/HIGGS.csv'
inputRDD=sc.textFile(path)
# inputRDD.first()


# In[15]:

# making problem binary  ##### problem is aHERE???

Data=inputRDD.map(lambda line: [float(x) for x in line.split(',')]).map(lambda V:LabeledPoint(V[0],V[1:]))


# In[16]:

Data1=Data.sample(False,0.1).cache()
(trainingData,testData)=Data1.randomSplit([0.7,0.3], seed=255)
#(trainingData,testData)=Data.randomSplit([0.7,0.3], seed=255)


# In[ ]:

#Gradient Boosted Trees ----------------------------------

from time import time
errors={}
for depth in [10]:
    start=time()
    model=GradientBoostedTrees.trainClassifier( trainingData,{}, numIterations=10, learningRate = 0.35, maxDepth = depth)  #Data???
    #print model.toDebugString()
    errors[depth]={}
    dataSets={'train':trainingData,'test':testData}
    for name in dataSets.keys():  # Calculate errors on train and test sets
        data=dataSets[name]
        Predicted=model.predict(data.map(lambda x: x.features))
        LabelsAndPredictions=data.map(lambda x: x.label).zip(Predicted) ### FILLIN ###
        Err = LabelsAndPredictions.filter(lambda (v,p):v != p).count()/float(data.count())
        errors[depth][name]=Err
    print depth,errors[depth]
#print errors



# In[14]:

# Random Forrests ----------------------------------------------------

# from time import time
# errors={}
# for depth in [10]: # included 15,20
#     start=time()
#     model = RandomForest.trainClassifier(trainingData, numClasses = 8, categoricalFeaturesInfo = {}, numTrees=30, featureSubsetStrategy='auto', impurity='gini', maxDepth=depth, maxBins=32, seed=None)
#     #print model.toDebugString()
#     errors[depth]={}
#     dataSets={'train':trainingData,'test':testData}
#     for name in dataSets.keys():  # Calculate errors on train and test sets
#         ### FILLIN ###
#         data=dataSets[name]
#         Predicted=model.predict(data.map(lambda x: x.features))
#         LabelsAndPredictions=data.map(lambda x: x.label).zip(Predicted)
#         Err = LabelsAndPredictions.filter(lambda (v,p):v != p).count()/float(data.count())
#         errors[depth][name]=Err
#     print depth,errors[depth],int(time()-start),'seconds'
# print errors



