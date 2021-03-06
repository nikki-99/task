from ctypes.wintypes import tagMSG
import yaml
import datetime
from yaml import Loader
from importlib.abc import Loader
import threading
import time

file = "DataSet/Milestone1/Milestone1A.yaml"


def entryLog(k):
    with open("Milestone1a_log.txt", 'a') as write_log:
        t = datetime.datetime.now()
        write_log.write(str(t) +";"+ str(k))
        write_log.write('\n')
        write_log.close()
        

def load_data(path):
    with open(path, 'r') as file_read:
        data = yaml.load(file_read, Loader=yaml.FullLoader)
        return data
    
data = load_data(file)


data = data['M1A_Workflow']

txt = "M1A_Workflow"

# txt="" 

# for key,val in data.items():
#     data = val
#     txt+=key

tasks = []

def find_task(txt,data):
    entryLog(txt + " Entry ")
    if data['Type'] == 'Flow':
        execution = data['Execution']
        activities = data['Activities']
        if execution == 'Sequential':
            for k, v in activities.items():
                find_task(txt + "." + k ,v)
        elif execution == 'Concurrent':
            threads = []
            for k, v in activities.items():
                t = threading.Thread(target=find_task, args=(txt + "." + k ,v,))
                threads.append(t)
            for t in threads:
                t.start()
                # time.sleep()
            for t in threads:
                t.join()
        entryLog(txt + " Exit")
    elif data['Type'] == 'Task':
        func_name = data['Function']
        finput = data['Inputs']['FunctionInput']
        exe_time = data['Inputs']['ExecutionTime']
        entryLog(txt + " Executing " + str(func_name) + " (" + str(finput) +", "+ str(exe_time) + ") ")
        tasks.append(data)
        time.sleep(int(exe_time))
        entryLog(txt + " Exit")
    
    
    

find_task(txt,data)

# print(tasks)

# t1 = threading.Thread(target=find_task, args=(txt,data,))
# t2 = threading.Thread(target=find_task,args=(txt,data,))

# t1.start()
# time.sleep(0.5)
# t2.start()

# t1.join()
# t2.join()
