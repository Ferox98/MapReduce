import requests
import os 
import time 

BASE = "http://127.0.0.1:5000"

class Worker:

    def __init__(self):
        self.files = []
    
    def getFiles(self):
        path = os.getcwd() + '\\intermediate'
        self.files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

    def work(self):
        while True:
            try:
                response = requests.get(BASE + "/driver")
                if response.json() is None:
                    print("No tasks to execute")
                    break
                params = response.json().split(',')
                if params[0] == 'None':
                    break
                elif params[0] == 'MAP': # map task
                    filename = params[2]
                    bucket_count = int(params[3])
                    print('Mapping {}'.format(filename))
                    task_id = params[1]
                    buckets = [] 
                    for i in range(bucket_count):
                        buckets.append([])
                    file = open('inputs/' + filename, 'r')
                    lines = file.readlines()
                    for line in lines:
                        # remove punctuation marks from sentences
                        translation_table = dict.fromkeys(map(ord, '!@$%,/\"\'?.()&:;'), None)
                        line = line.translate(translation_table)
                        line = line.strip()
                        words = line.split(' ')
                        for word in words:
                            if len(word) == 0:
                                continue
                            cur_word = word.lower()
                            # bucket is assigned on ASCII value of first character modulo the number of buckets (reduce tasks)
                            bucket = ord(cur_word[0]) % bucket_count
                            buckets[bucket].append(word)
                    # write buckets to file
                    for i in range(len(buckets)):
                        filename = 'intermediate/mr-{}-{}.txt'.format(task_id, i)
                        with open(filename, 'w') as f:
                            for word in buckets[i]:
                                if len(word) > 0:
                                    f.write(word + "\n")
                else: # reduce task
                    if len(self.files) == 0:
                        self.getFiles()
                    reduce_task_id = params[1]
                    print('Reducing bucket {}'.format(reduce_task_id)) 
                    word_count = {}
                    for filename in self.files:
                        if filename[-5] == reduce_task_id:
                            file = open('intermediate/' + filename, 'r')
                            lines = file.readlines()
                            for word in lines:
                                word = word.strip()
                                if word.lower() not in word_count:
                                    word_count[word.lower()] = 1
                                else:
                                    word_count[word.lower()] += 1
                    file = open('out/out-' + reduce_task_id + '.txt', 'w')
                    for word in word_count:
                        file.write(word + ' ' + str(word_count[word]) + '\n')
            except requests.exceptions.ConnectionError as e:
                print('No tasks to execute')
                break

if __name__ == "__main__":    
    worker = Worker()
    worker.work()