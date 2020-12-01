import sys
import os
import glob
import datetime
from datetime import datetime as da
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

jmeans=[]
jmedians=[]
tmeans=[]
tmedians=[]
master_filename='logs_master_'
workers_filename='logs_worker_'
algo=['LL','RANDOM','RR']
results_path=os.getcwd()
if(results_path[len(results_path)-1]!='/'):
	results_path+='/'
dformat="%Y-%m-%d %H:%M:%S.%f"
'''Parses the master and worker log files for each of the algorithm and appends the values to a dictionary,
	for ease of plotting and calculating mean and median job and task completion times'''
for name in algo:
	t=results_path+workers_filename+'*'+name+'.txt'
	workers=glob.glob(t)
	print('\nMaster',name+'\n')
	jobs={}
	su=da.strptime('00:00:00.000000','%H:%M:%S.%f')
	with open(master_filename+name+'.txt','r') as f:
		d={}
		for j in f.readlines():
			l=j.split(",")
			if(l[0] not in d.keys()):
				d[l[0]]=[(l[1],l[2])]
			else:
				d[l[0]].append((l[1],l[2]))
	for i in d.keys():
		start_time=d[i][0][1][:len(d[i][0][1])-1]
		end_time=d[i][1][1][:len(d[i][1][1])-1]
		start_time=da.strptime(start_time,dformat)
		end_time=da.strptime(end_time,dformat)
		t=end_time-start_time
		jobs[i]=t
		su+=t
	s=jobs[str(len(jobs)//2)]
	print('Median Job Completion',s.total_seconds())
	ms=float(su.strftime('.%f'))
	ms+=float(su.hour*10000+su.minute*100+su.second)
	print('Mean Job Completion',ms/len(jobs))
	jmeans.append(ms/len(jobs))
	jmedians.append(s.total_seconds())
	worker=[]
	d={}
	su=da.strptime('00:00:00.000000','%H:%M:%S.%f')
	wc=1
	for i in workers:
		with open(i,'r') as f:
			lines=f.readlines()
			worker.append(len(lines)//2)
			for j in lines:
				l=j.split(",")
				if((l[0],l[1],wc) not in d.keys()):
					d[(l[0],l[1],wc)]=[l[3]]
				else:
					d[(l[0],l[1],wc)].append(l[3])
		wc+=1
	le=len(d)//2
	count=0
	print('\nWORKERS '+name+'\n')
	worker_t={}
	for i in d.keys():
		start_time=d[i][0][:len(d[i][0])-1]
		end_time=d[i][1][:len(d[i][1])-1]
		start_time=da.strptime(start_time,dformat)
		end_time=da.strptime(end_time,dformat)
		t=end_time-start_time
		if(count==le):
			print('Median Task Completion',t.total_seconds())	
			tmedians.append(t.total_seconds())
		count+=1
		su+=t
		if i[2] not in worker_t:
			worker_t[i[2]]=t
		else:
			worker_t[i[2]]+=t
	times=list(i.total_seconds() for i in worker_t.values())
	print(times,worker)
	'''Plot for visualizing the number of tasks scheduled per worker against time for each algorithm.'''
	plt.plot(worker,times,marker='o')
	plt.ylabel('TIME')
	plt.xlabel('NO OF TASKS PER WORKER')
	plt.title(name)
	plt.grid()
	for i_x,i_y in zip(worker,times):
		plt.text(i_x,i_y,'({}, {})'.format(i_x,i_y))
	plt.show()
	plt.close()
	ms=float(su.strftime('.%f'))
	ms+=float(su.hour*10000+su.minute*100+su.second)
	print('Mean Task Completion',ms/len(d))
	tmeans.append(ms/len(d))

label1=['mean' for i in jmeans]
label2=['median' for i in jmedians]
algo=algo+algo
label=label1+label2
'''Grouped Bar chart for visualizing the mean and median job completion time for all the algorithms.'''
nums1=jmeans+jmedians
df1 = pd.DataFrame(list(zip(algo,nums1,label)), columns =['Algorithm','Time','metrics'])
sns.barplot(x = 'Algorithm', y = 'Time', hue = 'metrics', data = df1)
plt.title("Job Completion Times")
plt.show()

'''Grouped Bar chart for visualizing the mean and median Task completion time for all the algorithms.'''
nums2=tmeans+tmedians
df2 = pd.DataFrame(list(zip(algo,nums2,label)), columns =['Algorithm','Time','metrics'])
sns.barplot(x = 'Algorithm', y = 'Time', hue = 'metrics', data = df2)
plt.title("Task Completion Times")
plt.show()







	
	
	
	
	
	
	
	
	
	
	
	
	
