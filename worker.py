import json
import socket
import time
import sys
import random
import numpy as np
import threading
from datetime import datetime

port_no=int(sys.argv[1])
worker_id=sys.argv[2]


class worker:
	def __init__(self):
		self.req()
	''' Function which decrements the duration of the task until 0 i.e executes the task and sends a update message to the master at port 5001'''
	def run1(self,task):
		duration=task[2]
		while(duration):
			duration-=1
			time.sleep(1)
			
		res=socket.socket()
		res.connect(("localhost",5001))
		s=','.join(str(i) for i in task)
		print("sending message task completed",task[1])
		with open("logs_worker_"+str(task[5])+'_'+str(task[7])+".txt",'a') as f:
				f.write(str(task[0])+","+str(task[1])+",task_end,"+str(datetime.now())+"\n")
		res.send(s.encode())
	''' Function to recieve the task execution message from the master at port specified as the command line argument, and spawns a thread with the function run1 to execute the task'''
	def req(self):
		req=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		req.bind(("localhost",port_no))          
		req.listen(12)
		while(True):
			conn2, addr2 = req.accept()
			task=conn2.recv(1024).decode()
			task=task.split(',')
			task[2]=int(task[2])
			task[4]=int(task[4])
			task[5]=int(task[5])
			task[6]=int(task[6])
			print("Got the request to execute task",task[1])
			with open("logs_worker_"+str(task[5])+'_'+str(task[7])+".txt",'a') as f:
				f.write(str(task[0])+","+str(task[1])+",task_start,"+str(datetime.now())+"\n")
			thread='thread'+str(task[6])
			thread=threading.Thread(target=self.run1,args=[task])
			thread.start()
st=worker()
