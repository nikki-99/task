from ctypes.wintypes import tagMSG
import yaml
import datetime
from yaml import Loader
from importlib.abc import Loader
import threading
import time

file = "DataSet/Milestone1/Milestone1B.yaml"


def entryLog(k):
    with open("Milestone2_log.txt", 'a') as write_log:
        t = datetime.datetime.now()
        write_log.write(str(t) +";"+ str(k))
        write_log.write('\n')
        write_log.close()
        

def load_data(path):
    with open(path, 'r') as file_read:
        data = yaml.load(file_read, Loader=yaml.FullLoader)
        return data
    
data = load_data(file)


data = data['M1B_Workflow']

txt = "M1B_Workflow"

tasks = []

def find_task(txt,data):
    entryLog(txt + " Entry ")
    if data['Type'] == 'Flow':
        execution = data['Execution']
        activities = data['Activities']
        for k, v in activities.items():
            if not v['Type']=='Task':
                entryLog(txt + "." + k + " Entry ")
                # entryLog(txt +  " Exit ")
            
            # if k.find('Task'):
            find_task(txt +"."+k,v)
    elif data['Type'] == 'Task':
        func_name = data['Function']
        func_input = data['Inputs']['FunctionInput']
        exe_time = data['Inputs']['ExecutionTime']
        entryLog(txt + " Executing " + str(func_name) + " ("+ str(func_input)+ ", "+str(exe_time)+")")
        entryLog(txt +  " Exit ")
        tasks.append(data)
    
    
    

# find_task(txt,data)

# print(tasks)

t1 = threading.Thread(target=find_task, args=(txt,data,))
# t2 = threading.Thread(target=find_task,args=(txt,data,))

t1.start()
time.sleep(0.5)
# t2.start()

t1.join()
# t2.join()
