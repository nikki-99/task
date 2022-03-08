from ctypes.wintypes import tagMSG
import yaml
import datetime
from yaml import Loader
from importlib.abc import Loader
import threading
import time
import csv

file = "DataSet/Milestone2/Milestone2A.yaml"


def entryLog(k):
    with open("Milestone2a_log.txt", 'a') as write_log:
        t = datetime.datetime.now()
        write_log.write(str(t) +";"+ str(k))
        write_log.write('\n')
        write_log.close()
        

def load_data(path):
    with open(path, 'r') as file_read:
        data = yaml.load(file_read, Loader=yaml.FullLoader)
        return data
    
data = load_data(file)


data = data['M2A_Workflow']

txt = "M2A_Workflow"

tasks = []

def load_csv(filename):
    with open(filename,"r")as f:
        datas = csv.reader(f)
        print(datas)
        for data in datas:
            print(data)


def find_task(txt,data):
    entryLog(txt + " Entry ")
    if data['Type'] == 'Flow':
        execution = data['Execution']
        activities = data['Activities']
        if execution == 'Sequential':
            for k, v in activities.items():
                find_task(txt + "." + k ,v)
        elif execution == 'Concurrent':
            thread_items = []
            for k, v in activities.items():
                t = threading.Thread(target=find_task, args=(txt + "." + k ,v,))
                thread_items.append(t)
            for t in thread_items:
                t.start()
            for t in thread_items:
                t.join()
        entryLog(txt + " Exit")
    elif data['Type'] == 'Task':
        func_name = data['Function']
        if func_name=='DataLoad':
            filename = data['Inputs']['Filename']
            load_csv('./DataSet/Milestone2/'+filename)
        elif func_name=='TimeFunction':
            con=''
            if len(data)>3:
                con = data['Condition']
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
