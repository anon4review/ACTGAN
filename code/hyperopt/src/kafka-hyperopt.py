import random

from hyperopt import fmin, tpe, hp, space_eval, rand, STATUS_OK, STATUS_FAIL, anneal
import sys, os, time, re
from subprocess import Popen, PIPE

import Command
import time
defaultConfPath = "/home/cloud/bayes_op/smac-v2.10.03-master-778/example_scenarios/spark-als/spark.conf"
targetConfPath = "/home/cloud/HiBench-master/conf/spark.conf"
cmd = "/home/cloud/HiBench-master/bin/workloads/ml/als/spark/run.sh"
starttime = time.time()
# defaultConfPath = "spark.conf"
# targetConfPath = "hh.conf"

def copyFile(source, target, configs):
    open(target, "w").write(open(source, "r").read())
    open(target, "a").write(configs)

def changeConfig(configs):
    configString = ""
    configString += ("\n" + "spark.memory.fraction" + " " + str(configs[0]))
    configString += ("\n" + "spark.memory.storageFraction" + " " + str(configs[1]))
    configString += ("\n" + "spark.task.cpus" + " " + str(configs[2]))
    configString += ("\n" + "spark.shuffle.compress" + " " + configs[3])
    configString += ("\n" + "spark.shuffle.spill.compress" + " " + configs[4])
    configString += ("\n" + "spark.reducer.maxSizeInFlight" + " " + str((configs[5] + 8)) + "m")
    configString += ("\n" + "spark.shuffle.file.buffer" + " " + str((configs[6] + 8)) + "k")
    configString += ("\n" + "spark.rdd.compress" + " " + configs[7])
    configString += ("\n" + "spark.io.compression.codec" + " " + configs[8])
    configString += ("\n" + "spark.serializer" + " " + configs[9])
    copyFile(defaultConfPath, targetConfPath, configString)


def objective(args):
    #
    print(args)
    ans = 0.0
    interval = time.time()-starttime
    # print(interval)
    if(interval>18500):
        sys.exit(0)
    try:
        ans = Command.getThrought(args,type='hyperopt')
    except Exception as e:
         return {'loss': -ans, 'status': STATUS_FAIL}
    return {'loss': -ans, 'status': STATUS_OK}

    # start_time = time.time()
    # io = Popen(cmd.split(" "), stdout=PIPE, stderr=PIPE, shell=True)
    # time.sleep(5)
    # (stdout_, stderr_) = io.communicate()
    # runtime = random.uniform(10, 20)

# define a search space
space = [
    hp.randint('num.network.threads', 20),
    hp.randint('num.io.threads', 24),
    hp.randint('queued.max.requests', 100),
    hp.randint('num.replica.fetchers', 20),
    hp.randint('socket.receive.buffer.bytes', 20),
    hp.randint('socket.send.buffer.bytes', 20),
    hp.randint('socket.request.max.bytes', 30),
    hp.randint('buffer.memory', 48),
    hp.randint('batch.size', 64),
    hp.randint('linger.ms', 100),
    hp.choice('compression.type', ['none', 'lz4', 'gzip', 'snappy']),#, 'gzip', 'snappy', 'lz4'
    hp.choice('acks', [1]),
]
def main():
    #default params
    # p = [2, 7, 9, 0, 9, 9, 9, 15, 3, 0, 'none', 1]
    # Command.getThrought(p)
    # minimize the objective over the space
    #tpe rand
    # global starttime
    # print(starttime)
    # time.sleep(3)
    # starttime = time.time()

    # print(starttime)
    best = fmin(objective, space, algo=tpe.suggest, max_evals=1000)

    print(best)
    # -> {'a': 1, 'c2': 0.01420615366247227}
    print(space_eval(space, best))
    # -> ('case 2', 0.01420615366247227)
    Command.getThrought(space_eval(space, best))
if __name__=="__main__":
    main()