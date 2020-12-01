import json
import socket
import time
import sys
import random
import numpy as np
import threading
import os
from datetime import datetime
random.seed(42)

class master:
	def __init__(self):
		self.jobs=[]
		self.workers=[]
		self.rr=0 
		self.q=[]
		self.sche_algo=sys.argv[2]
		self.config_path=sys.argv[1]
		self.lock=threading.Lock()
		
		
		with open(self.config_path,'r') as f:
			text=''
			for i in f.readlines():
				text+=i
			text=json.loads(text)
			for i in text['workers']:
				self.lock.acquire()
				self.workers.append(i)
				self.lock.release()
				'''thread_name=str(i['worker_id'])+'th'
				thread_name=threading.Thread(target=self.dummy,args=[str(i['port']),str(i['worker_id'])])
				thread_name.start()'''
		print("Workers succesfully opened and are listening")
	def dummy(self,port,wid):
		os.system('python3 worker.py '+port+' '+wid)
	def round_robin(self,task):
		if(task[4]==1):
			return
		if(self.workers[self.rr]['slots']!=0):
			print("scheduling to worker", self.workers[self.rr]['worker_id'], 'free')
			for i in self.q:
				if(task[1]==i[1]):
					task[4]=1
					self.lock.acquire()
					i[4]=1
					self.lock.release()
					break	
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
				s.connect(("localhost", int(self.workers[self.rr]['port'])))
				print("element",task,"In q and it is a",task[3],"task")
				message=str(task[0])+','+str(task[1])+','+str(task[2])+','+str(task[3])+','+str(task[4])+ ',' + str(self.workers[self.rr]['worker_id'])+','+str(self.workers[self.rr]['slots'])+','+str(self.sche_algo)
				s.send(message.encode())
			self.lock.acquire()
			self.workers[self.rr]['slots']-=1
			self.lock.release()
			self.lock.acquire()
			self.rr=(self.rr+1)%len(self.workers)
			self.lock.release()
		else:
			self.lock.acquire()
			self.rr=(self.rr+1)%len(self.workers)
			self.lock.release()
			self.round_robin(task)
		return

	def free_worker(self):
		max_slots=0
		max_id=self.workers[0]['worker_id']
		max_i=0
		count=0
		for i in self.workers:
			if(i['slots']>max_slots):
				max_slots=i['slots']
				max_id=i['worker_id']
				max_i=count
			count+=1
		return (max_i,max_id,max_slots)
			
	def least_load(self,task):
		while(True):
			if(task[4]==1):
				return
			free_worker_index,free_worker_id, free_slots = self.free_worker()
			if(free_slots!=0):
				print("found worker", free_worker_id, 'free')
				for i in self.q:
					if(task[1]==i[1]):
						task[4]=1
						self.lock.acquire()
						i[4]=1
						self.lock.release()
						break
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
					s.connect(("localhost", int(self.workers[free_worker_index]['port'])))
					print("element",task,"In q and it is a",task[3],"task")
					message=str(task[0])+','+str(task[1])+','+str(task[2])+','+str(task[3])+','+str(task[4])+ ',' + str(self.workers[free_worker_index]['worker_id'])+','+str(self.workers[free_worker_index]['slots'])+','+str(self.sche_algo)
					s.send(message.encode())
				self.lock.acquire()
				self.workers[free_worker_index]['slots']-=1
				self.lock.release()
				return
			else:
				time.sleep(1)

	def random_algo(self,task):
		while(True):
			if(task[4]==1):
				return
			rand_worker=random.choice(self.workers)
			rand_worker_index=0
			for i in self.workers:
				if(rand_worker['worker_id']==i['worker_id']):
					break
				rand_worker_index+=1
			if(rand_worker['slots']!=0):
				print("found worker", rand_worker['worker_id'], 'free')
				for i in self.q:
					if(task[1]==i[1]):
						task[4]=1
						self.lock.acquire()
						i[4]=1
						self.lock.release()
						break
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
					s.connect(("localhost", int(rand_worker['port'])))
					print("element",task,"In q and it is a",task[3],"task")
					message=str(task[0])+','+str(task[1])+','+str(task[2])+','+str(task[3])+','+str(task[4])+ ',' + str(self.workers[rand_worker_index]['worker_id'])+','+str(self.workers[rand_worker_index]['slots'])+','+str(self.sche_algo)
					s.send(message.encode())
				self.lock.acquire()
				self.workers[rand_worker_index]['slots']-=1
				self.lock.release()
				return
	def assign(self):
		while(True):
			for j in self.q:
				if(j[3]=='r'):
					for k in self.jobs:
						if(k[0]['job_id']==j[0]):
							if(k[1]==0):
								if(self.sche_algo=="RR"):
									self.round_robin(j)
								elif(self.sche_algo=="LL"):
									self.least_load(j)
								elif(self.sche_algo=="RANDOM"):
									self.random_algo(j)
							break
				else:
					#print("map task")
					if(self.sche_algo=="RR"):
						self.round_robin(j)
					elif(self.sche_algo=="LL"):
						self.least_load(j)
					elif(self.sche_algo=="RANDOM"):
						self.random_algo(j)

	def req(self):
		r=socket.socket()
		r.bind(("localhost",5000)) 
		count=0     
		r.listen(4)
		while(True):
			count+=1
			conn1, addr1 = r.accept()
			job_request=conn1.recv(1024).decode()
			job_request=json.loads(job_request)
			no_of_maps=len(job_request['map_tasks'])
			no_of_red=len(job_request['reduce_tasks'])
			self.lock.acquire()
			with open("logs_master_"+str(self.sche_algo)+".txt",'a') as f:
				f.write(str(job_request['job_id'])+",job_start,"+str(datetime.now())+"\n")
			self.jobs.append([job_request,no_of_maps,no_of_red])
			self.lock.release()
			#self.scheduler(job_request)
			for i in job_request['map_tasks']:
				self.lock.acquire()
				self.q.append([job_request['job_id'],i['task_id'],i['duration'],'m',0])
				self.lock.release()
			for i in job_request['reduce_tasks']:
				self.lock.acquire()
				self.q.append([job_request['job_id'],i['task_id'],i['duration'],'r',0])
				self.lock.release()

	def worker(self):
		w=socket.socket()
		w.bind(("localhost",5001))          
		w.listen(12)
		while(True):
			conn2, addr2 = w.accept()
			worker_update=conn2.recv(1024).decode()
			worker_update=worker_update.split(',')
			worker_update[2]=int(worker_update[2])
			worker_update[4]=int(worker_update[4])
			worker_update[5]=int(worker_update[5])
			worker_update[6]=int(worker_update[6])
			count=0
			for i in self.q:
				if(i[0]==worker_update[0]):
					count+=1
			if(count==1):
				with open("logs_master_"+str(self.sche_algo)+".txt",'a') as f:
					f.write(str(worker_update[0])+",job_end,"+str(datetime.now())+"\n")
			self.lock.acquire()
			self.q.remove(worker_update[:5])
			self.lock.release()
			idx=0
			for i in self.workers:
				if(worker_update[5]==i['worker_id']):
					break
				idx+=1
			self.lock.acquire()
			self.workers[idx]['slots']+=1
			self.lock.release()
			for i in self.jobs:
				if(i[0]['job_id']==worker_update[0]):
					if(worker_update[3]=='m'):
						self.lock.acquire()
						i[1]-=1
						self.lock.release()
		
	def main(self):
		job_requests=threading.Thread(target=self.req)
		job_requests.start()
		worker_updates=threading.Thread(target=self.worker)
		worker_updates.start()
		thread_assign=threading.Thread(target=self.assign)
		thread_assign.start()
		job_requests.join()
		worker_updates.join()
		thread_assign.join()
s=master()
s.main()
