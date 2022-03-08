from ctypes.wintypes import tagMSG
import yaml
import datetime
from yaml import Loader
from importlib.abc import Loader
import threading
import time

file = "DataSet/Milestone1/Milestone1B.yaml"


def entryLog(k):
    with open("Milestone1b_log.txt", 'a') as write_log:
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
        if execution == 'Sequential':
            for k, val in activities.items():
                find_task(txt + "." + k ,val)
        elif execution == 'Concurrent':
            threads = []
            for k, val in activities.items():
                t = threading.Thread(target=find_task, args=(txt + "." + k ,val,))
                threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        entryLog(txt +  " Exit ")
    elif data['Type'] == 'Task':
        func_name = data['Function']
        func_input = data['Inputs']['FunctionInput']
        exe_time = data['Inputs']['ExecutionTime']
        entryLog(txt + " Executing " + str(func_name) + " ("+ str(func_input)+ ", "+str(exe_time)+")")
        time.sleep(int(exe_time))
        entryLog(txt +  " Exit ")
        tasks.append(data)
    
    
    

find_task(txt,data)

# print(tasks)
# if __name__ =='__main__':
# t1 = threading.Thread(target=find_task, args=(txt,data,))
# t2 = threading.Thread(target=find_task,args=(txt,data,))

# t1.start()
# time.sleep(0.5)
# t2.start()

# t1.join()
# t2.join()
