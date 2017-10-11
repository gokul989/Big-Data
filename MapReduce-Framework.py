import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random
from collections import defaultdict
import math

PERMITTED_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" 

##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:#[DONE]
	__metaclass__ = ABCMeta
	
	def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3): 
		self.data = data  #the "file": list of all key value pairs
		self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
		self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks


	@abstractmethod
	def map(self, k, v): 
		print("Need to override map")

	
	@abstractmethod
	def reduce(self, k, vs): 
		print("Need to override reduce")
		

	###########################################################
	#System Code: What the map reduce backend handles

	def mapTask(self, data_chunk, namenode_m2r): 
		#runs the mappers and assigns each k,v to a reduce task
		for (k, v) in data_chunk:
			#run mappers:
			mapped_kvs = self.map(k, v)
			#print(mapped_kvs)
			#assign each kv pair to a reducer task
			for (k, v) in mapped_kvs:
				namenode_m2r.append((self.partitionFunction(k), (k, v)))


	def partitionFunction(self,k): 
		#given a key returns the reduce task to send it
		if(isinstance(k,(int))):
			return k%self.num_reduce_tasks
		
		return (len(k)%self.num_reduce_tasks)

	def reduceTask(self, kvs, namenode_fromR): 
		#sort all values for each key (can use a list of dictionary)
		list1 = []
		dic = defaultdict(list)
		#print(kvs)
		for i in range(len(kvs)):
			dic[kvs[i][0]].append(kvs[i][1])
		#print(dic.items())
		#kvs.sort(key=lambda x: x[0])

		#call reducers on each key with a list of values
		#and append the result for each key to namenode_fromR
		
		for k, v in dic.items():
			reduced_kvs = self.reduce(k, v)		
			if(reduced_kvs != None):
				namenode_fromR.append(reduced_kvs)
		#pass
		
	def runSystem(self): 
		#runs the full map-reduce system processes on mrObject

		#the following two lists are shared by all processes
		#in order to simulate the communication
		
		namenode_m2r = Manager().list() #stores the reducer task assignment and 
										  #each key-value pair returned from mappers
										  #in the form: [(reduce_task_num, (k, v)), ...]
		namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
										#in the form [(k, v), ...]
		
		#divide up the data into chunks accord to num_map_tasks, launch a new process
		#for each map task, passing the chunk of data to it. 
		#if chunk contains the data going to a given maptask then the following
		#	   starts a process
		#	   p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
		#	   p.start()  
		
		
		processList = []
		f = math.ceil(len(self.data)/self.num_map_tasks)
		for i in range(0, len(self.data),f ):        
			chunk = self.data[i:i+f]
			#print(chunk)
			p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
			p.start()
			processList.append(p)
			
		
		
		#join map task processes back
		
		for proc in processList:
			proc.join()
		
		#print output from map tasks 
		
		print("namenode_m2r after map tasks complete:")
		pprint(sorted(list(namenode_m2r)))

		#"send" each key-value pair to its assigned reducer by placing each 
		#into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
		to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
		for i in range(len(namenode_m2r)):
			to_reduce_task[namenode_m2r[i][0]].append(namenode_m2r[i][1])
			
		
		

		#launch the reduce tasks as a new process for each. 
		
		f = math.ceil(len(namenode_m2r)/self.num_reduce_tasks)
		processList = []
		for i in range(self.num_reduce_tasks):        
			#chunk = namenode_m2r[i:i+f]
			#print(chunk)
			p = Process(target=self.reduceTask, args=(to_reduce_task[i],namenode_fromR))
			p.start()
			processList.append(p)


		#join the reduce tasks back
		
		for proc in processList:
			proc.join()
		
		#print output from reducer tasks 
		
		print("namenode_m2r after reduce tasks complete:")
		pprint(sorted(list(namenode_fromR)))

		#return all key-value pairs:
		
		return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:
			
class WordCountMR(MyMapReduce): 
	#the mapper and reducer for word count
	def map(self, k, v): 
		counts = dict()
		for w in v.split():
			w = "".join(c for c in w if c in PERMITTED_CHARS)
			if len(w)==0:
				continue
			w = w.lower() 
			try:  
				counts[w] += 1
			except KeyError:
				counts[w] = 1
		return counts.items()
	
	def reduce(self, k, vs): 
		
		return (k, np.sum(vs))		  
	

class SetDifferenceMR(MyMapReduce): 
	#contains the map and reduce function for set difference
	#Assumes that the mapper receives the "set" as a list of any primitives or comparable objects
	#pass
	def map(self,k,v):
		counts=dict()		
		for i in range(len(v)):
			counts[v[i]] = k
			#print(v[i] , k)		
		return counts.items()
	def reduce(self,k,vs):
		if(len(vs)==1 and vs[0] =='R'):
			return k


##########################################################################
##########################################################################


if __name__ == "__main__": 
	###################
	##run WordCount:
	data = [(1, "The horse raced past the barn fell"),
			(2, "The complex houses married and single soldiers and their families"),
			(3, "There is nothing either good or bad, but thinking makes it so"),
			(4, "I burn, I pine, I perish"),
			(5, "Come what come may, time and the hour runs through the roughest day"),
			(6, "Be a yardstick of quality."),
			(7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
			(8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
			(9, "The car raced past the finish line just in time."),
			(10, "Car engines purred and the tires burned.")]
	mrObject = WordCountMR(data, 4, 3)
	mrObject.runSystem()
			 
	####################
	##run SetDifference
	
	print("\n\n*****************\n Set Difference\n*****************\n")
	data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
			 ('S', ['pear' , 'orange', 'strawberry', 'fig', 'tangerine'])]
	data2 = [('R', [x for x in range(50) if random() > 0.5]),
			 ('S', [x for x in range(50) if random() > 0.75])]
	mrObject = SetDifferenceMR(data1, 2, 2)
	mrObject.runSystem()
	mrObject = SetDifferenceMR(data2, 2, 2)
	mrObject.runSystem() 

	  
