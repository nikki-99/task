from importlib.abc import Loader
import yaml
from threading import *
import datetime
import time
import csv


def TimeFunction(seconds):
    time.sleep(seconds)

def entry_log(k):
    with open("Milestone2a_log.txt", 'a') as write_log:
        t = datetime.datetime.now()
        write_log.write(str(t) + ";" + str(k))
        write_log.write('\n')
        write_log.close()
        

def load_data(path):
    with open(path, 'r') as file_read:
        data = yaml.load(file_read, Loader=yaml.FullLoader)
        return data
    
file = "DataSet/Milestone2/Milestone2A.yaml"

data = load_data(file)
data = data['M2A_Workflow']

txt = "M2A_Workflow"

tasks = []
dict = {}

def find_task(txt,data):
    entry_log(txt + " Entry ")
    if data['Type'] == 'Flow':
        execution = data['Execution']
        activities = data['Activities']
        if execution == 'Sequential':
            for k, v in activities.items():
                find_task(txt + "." + k ,v)
        elif execution == 'Concurrent':
            thread_items = []
            for k, v in activities.items():
                t = Thread(target=find_task, args=(txt + "." + k ,v,))
                thread_items.append(t)
            for t in thread_items:
                t.start()
            for t in thread_items:
                t.join()
    elif data['Type'] == 'Task':
        func = data['Function']
        if "Condition" in data:
            cond = data['Condition'].split(' ')
            key = cond[0]
            val = cond[2]
            if cond[1] == '<' and int(dict[key]) >= int(val):
                entry_log(txt+" Skipped")
                
            elif cond[1]=='>' and int(dict[key])<=int(val):
                entry_log(txt+" Skipped")
                pass
            
        if func == "TimeFunction":
            finput = data['Inputs']['FunctionInput']
            ftime = data['Inputs']['ExecutionTime']
            entry_log(txt + " Executing " + str(func) + " (" + str(finput) + ", " + str(ftime) + ")")
            TimeFunction(int(ftime))
        elif func == "DataLoad":
            fname = data['Inputs']['Filename']
            filepath = "DataSet/Milestone2/" + fname
            with open(filepath,"r")as f:
                csvdata = csv.reader(f)
                nofdefect = 0
                for data in csvdata:
                    nofdefect += 1
                key = "$(" + txt + ".NoOfDefects" + ")"
                dict[key] = nofdefect
        tasks.append(data)
        entry_log(txt + " Exit")


find_task(txt, data)

# print(dict)
# print(tasks)