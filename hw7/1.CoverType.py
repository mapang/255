# -*- coding: utf-8 -*-
#Name: Michelle Pang
#Email: mapang@eng.ucsd.edu
#PID: A10660264

from pyspark import SparkContext
sc = SparkContext()





from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

from string import split,strip

from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.tree import RandomForest, RandomForestModel  #added
from pyspark.mllib.util import MLUtils



# In[ ]:

# Read the file into an RDD  #THIS IS WHAT YOU REPLACE WHEN TURNING INTO PYOBLT
# If doing this on a real cluster, you need the file to be available on all nodes, ideally in HDFS.
path='/covtype/covtype.data'
inputRDD=sc.textFile(path) #inputRDD=sc.textFile(path).sample(False,0.1).cache()
#inputRDD.first()


# In[ ]:

# Transform the text RDD into an RDD of LabeledPoints NON BINARY ONE
#Data=inputRDD.map(lambda line: [float(strip(x)) for x in line.split(',')])    .map(lambda x: LabeledPoint(x[-1], x[:-2]))
#Data.first()
        


Label=2.0
Data=inputRDD.map(lambda line: [float(x) for x in line.split(',')]).map(lambda V:LabeledPoint(1.0,V[:-2]) if V[-1] == Label else LabeledPoint(0.0,V[:-2])).cache()


# ### Reducing data size ---------------------------------


#Data1=Data.sample(False,0.1).cache()
#(trainingData,testData)=Data1.randomSplit([0.7,0.3], seed=255)
(trainingData,testData)=Data.randomSplit([0.7,0.3], seed=255)




from time import time
errors={}
for depth in [10]:
    start=time()
    model=GradientBoostedTrees.trainClassifier( trainingData,{}, numIterations=13, learningRate = 0.27, maxDepth = depth)  #Data???
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



# ### ----------------------------------- Random Forests ----------------------------------------------
# 

# Data1=Data.sample(False,0.1).cache()
# (trainingData_H,testData_H)=Data1.randomSplit([0.7,0.3], seed=255)



# from time import time
# errors={}
# for depth in [15]: # included 15,20
#     start=time()
#     model = RandomForest.trainClassifier(trainingData, numClasses = 2, categoricalFeaturesInfo = {}, numTrees=17, featureSubsetStrategy='auto', impurity='gini', maxDepth=depth, maxBins=32, seed=None)
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


# In[ ]:

#RF_10trees = errors


# In[ ]:



