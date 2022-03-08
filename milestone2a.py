from importlib.abc import Loader
import threading
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
dt = {}


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
            for k, value in activities.items():
                if 'Condition' in value:
                    cond = value['Condition'].split(' ')
                    key = cond[0]
                    val = cond[2]
                    key1 = int(dt[key])
                    val1 = int(val)
                    # print(v1, v2)
                    if cond[1] == '>' and key1 <= val1:
                        entryLog(txt + "." + k + " Entry")
                        entryLog(txt + "." + k + " Skipped")
                        entryLog(txt + "." + k + " Exit")
                        continue
                    elif cond[1] == '<' and key1 >= val1:
                        entryLog(txt + "." + k + " Entry")
                        entryLog(txt + "." + k + " Skipped")
                        entryLog(txt + "." + k + " Exit")
                        continue
                # find_task(txt + "." + k ,v)
                t = Thread(target=find_task, args=(txt + "." + k ,value,))
                threads.append(t)
            for t in threads:
                # threading.Lock().acquire()
                t.start()
                time.sleep(1)
                # threading.Lock().release()
            for t in threads:
                t.join()
    elif data['Type'] == 'Task':
        func = data['Function']
        if func == "TimeFunction":
            finput = data['Inputs']['FunctionInput']
            exe_time = data['Inputs']['ExecutionTime']
            if finput in dt:
                entryLog(txt + " Executing " + str(func) + " (" + str(dt[finput]) + ", " + str(exe_time) + ")")
            else:
                entryLog(txt + " Executing " + str(func) + " (" + str(finput) + ", " + str(exe_time) + ")")
            time.sleep(int(exe_time))
        elif func == "DataLoad":
            fname = data['Inputs']['Filename']
            filepath = "DataSet/Milestone2/" + fname
            entryLog(txt + " Executing " + str(func)+" ("+fname+")")
            with open(filepath,"r")as f:
                csvdata = csv.reader(f)
                noOfdefect = 0
                for data in csvdata:
                    noOfdefect += 1
                key = "$(" + txt + ".NoOfDefects" + ")"
                dt[key] = noOfdefect
                print(key, noOfdefect)
        tasks.append(data)
    entryLog(txt + " Exit")


find_task(txt, data)

# print(dict)
# print(tasks)