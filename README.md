# MapReduce
Code for distributed MapReduce coding challenge by benshi.ai

Requirements:
The code uses Flask's REST API and requests module
pip install -r requirements.txt

Usage:
First run driver and specify N and M values as command line arguments.

python driver.py -N 6 -M 4

Then run workers on separate terminal.

python worker.py

Description:
The Driver module exposes an API that assigns a task to a worker. 
It first assigns map tasks to the workers to process the input files.
Once the map tasks are finished, the workers are then assigned reduce tasks.
When there are no more reduce tasks, the server shuts down and the driver
exits. 

The worker module tries to connect to the server's address to get a task
from the driver. If it's unable to do so, it concludes that there are no
tasks to execute and shuts down. Otherwise, it determines the nature of
the task from the response body. If it's assigned a map task, it reads
the file it was assigned, breaks the sentences down into words and writes
them to a bucket. As instructed, the bucket of the word is determined by
the ASCII value of the first character modulo the number of buckets. 
If however the worker is assigned a reduce task, it aggregates the count of
the words in the bucket it was assigned. Lastly, it writes to file the words
and their aggregated counts.

