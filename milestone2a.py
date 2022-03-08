from importlib.abc import Loader
import yaml
from threading import *
import datetime
import time
import csv




def entryLog(k):
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
            k = cond[0]
            val = cond[2]
            
            if cond[1] == '<' and int(dict[k]) >= int(val):
                entryLog(txt+" Skipped")
                return
            elif cond[1]=='>' and int(dict[k])<=int(val):
                entryLog(txt+" Skipped")
                return
                

            
        if func == "TimeFunction":
            finput = data['Inputs']['FunctionInput']
            exe_time = data['Inputs']['ExecutionTime']
            # for key,val in dict.items():
            #     print(key)
            # print(val)
            print(txt)
            entryLog(txt + " Executing " + str(func) + " (" + str(finput) + ", " + str(exe_time) + ")")
            time.sleep(int(exe_time))
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
                print(key)
            entryLog(txt + " Executing " + str(func)+" ("+fname+")")
            # time.sleep(int(exe_time))
        tasks.append(data)
    entryLog(txt + " Exit")


find_task(txt, data)

# print(dict)
# print(tasks)