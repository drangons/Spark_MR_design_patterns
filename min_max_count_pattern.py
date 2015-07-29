# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 23:00:44 2015

@author: dikshith



Summarization pattern
Problem: Given a list of user’s comments, determine the first and last time a
         user commented and the total number of comments from that user. 
         
 Dataset: cs.stackoverflow comments
 Example row: 
 <row Id="19" PostId="6" Score="1" 
 Text="@Janoma, you don't really need the recursive implementation.
 For example, if you look at the implementation of the qsort function in C,
 it does not use recursive calls, and therefore the implementation becomes much
 faster." CreationDate="2012-03-06T19:34:00.080" UserId="41" />
 
 Algorithm:
 1. Read the input data
 2. In map phase : We emit (userid,mindate,maxdate,count) tuple. Count =1, 
     minDate=maxDate=CreationDate
 3. Reduce phase: we compute tuple (userid, min(list of mindates), 
 max(max of list ..), sum(list of count))
 
 We count statistics without using the groupByKey function
 
 Note: BeautifulSoup is acting funny, not as documented in API
 
 TODO: You can use Named tuple instead of indexing. or sql Row attribute.
"""

import matplotlib.pyplot as plt
import os
from __future__ import division,print_function
from bs4 import BeautifulSoup
import datetime
from operator import add
from pyspark import SparkContext as sc




def getminmaxcount(record):
    """
    Takes a input record of type xml element. If there is a row attribute then
    emits the tuple (userid,mindate,maxdate,1). The first and last lines doesn't 
    contain row attribute because of <!DOCTYPE ..> tag
    
    Input: record: xml element 
    Output: tuple if the line contains row tag
    else just junk data so that next transformation depending on data structure 
    doesn't fail with NoneType is not iterable.
    """    
    raw = BeautifulSoup(record,'lxml')
    #if the parser contains row tag
    row = raw.row
    if row:
        date = datetime.datetime.strptime(row.get('creationdate'),
                                          "%Y-%m-%dT%H:%M:%S.%f")
        return (row.get('userid'), (date,date,1))
    else:
        return ('None',(datetime.datetime.now(),datetime.datetime.now(),1))
    # BeautifulSoup is really strange, the attribute is converted to lower case and get_text is not working 



"""
Read the input 
"""
baseDir = os.path.join(os.getcwd(),"stackoverflow")
PostingPath = os.path.join(baseDir,"PostHistory.xml")
commentspath = os.path.join(baseDir,"Comments.xml")


rawData = sc.textFile(commentspath,4)

print(rawData.take(1))

# tranform and reduce the data
output = rawData.map(getminmaxcount).reduceByKey(
lambda x,y: (min(x[0],y[0]),max(x[1],y[1]),add(x[2],y[2])))

print(output.take(5))

# convert the date back to string format for prinitng
finalOutput = output.map(lambda row: (row[0],
                                     row[1][0].strftime('%Y-%m-%dT%H:%M:%S.%f'),
                                     row[1][1].strftime('%Y-%m-%dT%H:%M:%S.%f'),
                                     row[1][2]))

#write the ouput to csv file, sortBy for only easy testing
with open('Pattern1_1.csv','w') as f:
    writer = csv.writer(f)
    writer.writerow(['id','mindate','maxdate','count'])
    writer.writerows(row for row in 
    finalOutput.sortBy(lambda x : x[-1],ascending=False).collect())

#testing
!cat  './stackoverflow/Comments.xml' | awk -F'"' '{print $12}' | grep -x '98' | wc -l