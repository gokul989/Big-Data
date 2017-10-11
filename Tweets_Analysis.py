import re
import os
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

sc =SparkContext()
PERMITTED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def f(x):
	#RegEx for getting the text withing "post" and "date" tags
    temp = re.findall("<post>(.*?)</post>", x[1], re.S) 
    temp2 = re.findall("<date>(.*?)</date>", x[1])
    tem =[]	
    for i in range(len(temp)):	
        j = [0]*len(broadcastVar.value)
        for word in temp[i].split(' '):
            if(word.lower() in broadcastVar.value):
                j[broadcastVar.value.index(word.lower())]+= 1
        for k in range(len(j)):
            if(j[k]>0):
                tem.append((broadcastVar.value[k],temp2[i],j[k]))
    return tem

if __name__ == "__main__":
	#RDD which reads all the files in the given path
	path ="blogs"
	textFiles = sc.wholeTextFiles(path)
	
	# RDD for getting the set of industries from each filename
	industries = textFiles.flatMap(lambda x: (os.path.basename(x[0]).split('.'))[3].split('-'))\
						   .map(lambda word: ("".join(c for c in word if c in PERMITTED_CHARS)).lower())\
						   .distinct()
	
	broadcastVar = sc.broadcast(industries.collect())	        
    
	#RDD of tuples in the form of <Industry_name,Date,count>
	list1 = textFiles.map(f).filter(lambda x:len(x)>=1).reduce(lambda x,y: x+y)
	counts = sc.parallelize(list1,numSlices=1000)
	
	
	
	
	#RDD containing the requring output <(industry_name)((date,count),(date,count))>
	toutput = counts.map(lambda lines:((lines[0],lines[1]),lines[2]))\
					.reduceByKey(lambda a,b:a+b)\
					.map(lambda lines:(lines[0][0],(lines[0][1],lines[1])))\
					.groupByKey()\
					.mapValues(tuple)					
	
	#print to screen
	for x in toutput.collect():
		print(x)
		print('')
	
	#print to file
	#toutput.coalesce(1).saveAsTextFile("a22_output.txt")