import os
import math 
from flask import Flask, request
from flask_restful import Api, Resource 
import sys, getopt

app = Flask(__name__)
api = Api(app)

class Driver():
    def __init__(self, N, M):
        path = os.getcwd() + '/inputs'
        self.files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        self.nodes_per_task = math.floor(len(self.files) / N)
        self.map_tasks = N
        self.reduce_tasks = M
        self.map_task_count = 0
        self.map_task_id = 0
        self.reduce_task_id = 0
        self.cur_file_idx = 0

    def getTask(self):
        if self.cur_file_idx < len(self.files):
            # we have more map tasks
            cur_task = 'MAP'
            cur_task_id = self.map_task_id
            res = (cur_task, cur_task_id, self.files[self.cur_file_idx], self.reduce_tasks)
            self.map_task_count += 1
            if self.map_task_count >= self.nodes_per_task and self.map_task_id != self.map_tasks - 1:
                self.map_task_id += 1
                self.map_task_count = 0
            self.cur_file_idx += 1
            return '{},{},{},{}'.format(res[0], res[1], res[2], res[3])
        elif self.reduce_task_id < self.reduce_tasks:
            cur_task = 'REDUCE' 
            res = (cur_task, self.reduce_task_id, None)
            self.reduce_task_id += 1
            return '{},{},{}'.format(res[0], res[1], res[2])
        else:
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None: 
                raise RuntimeError('Not running')
            func()


class DriverApi(Resource):
    def __init__(self, driver):
        self.driver = driver

    def get(self):
        return driver.getTask()


if __name__ == "__main__":
    N, M = None, None
    opts, args = getopt.getopt(sys.argv[1:], "hN:M:")
    for opt, arg in opts:
        if opt == '-N':
            N = int(arg)
        elif opt == '-M':
            M = int(arg)
    if N is None or M is None:
        print('Usage: driver.py -N <Value of N> -M <value of M>')
    else:
        driver = Driver(N, M)
        api.add_resource(DriverApi, "/driver", resource_class_kwargs={'driver': driver})
        app.run(debug=True)