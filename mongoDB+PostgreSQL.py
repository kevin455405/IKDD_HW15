import psycopg2
import threading
import time, itertools
import os
import pymongo
from pymongo import Connection    


#mongoDB
con = Connection('localhost', 27017)
db = con.test_database
collection = db.test_collection
posts = db.post

tStart = time.time()
class myThread_1 (threading.Thread):
    def __init__(self, threadID, name, args):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.args = args
    def run(self):
        #print "Starting " + self.name
        # Get lock to synchronize threads
        threadLock.acquire()
        insert_1(self.name, self.args)
        # Free lock to release next thread
        threadLock.release()

def insert_1(threadName, args):
    for root, dirs, files in os.walk("Data/"+args):
        #print root
        for f in files:
            path = os.path.join(root, f)
            f = open(path, 'r')
            for i in range(6):
                a=f.readline()

            while 1:
                a=f.readline()
                if a=='': break
                substr=a.split(',')
                post_i = {"Uid" : args,
                      "Date": substr[5],
                      "Time": substr[6],
                      "Lat" : substr[0],
                      "Lon" : substr[1]}
                posts.insert(post_i)

threadLock = threading.Lock()
threads = []

# Create new threads
thread1 = myThread_1(1, "Thread-1", "000")
thread2 = myThread_1(2, "Thread-2", "001")
thread3 = myThread_1(3, "Thread-3", "002")
thread4 = myThread_1(4, "Thread-4", "003")
thread5 = myThread_1(5, "Thread-5", "004")
thread6 = myThread_1(6, "Thread-6", "005")
# Start new Threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
# Add threads to thread list
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)
threads.append(thread5)
threads.append(thread6)
# Wait for all threads to complete
for t in threads:
    t.join()
#print "Exiting Main Thread"

tStop = time.time()
print "the insert time with MongoDB:" + `(tStop - tStart)`

tStart = time.time()
posts.find({"Uid": "003", "Date": "2008-11-19"}).sort("Time"):

tStop = time.time()
print "the query time with MongoDB:" + `(tStop - tStart)`


#psql      
conn = psycopg2.connect("dbname='test' user='postgres' host='localhost' password='0000'")
cur = conn.cursor()
cur.execute("CREATE TABLE test (Uid CHAR(5), Date CHAR(15), Time CHAR(10), Lat decimal, Lon decimal);")

tStart = time.time()
class myThread_2 (threading.Thread):
    def __init__(self, threadID, name, args):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.args = args
    def run(self):
        #print "Starting " + self.name
        # Get lock to synchronize threads
        threadLock.acquire()
        insert_2(self.name, self.args)
        # Free lock to release next thread
        threadLock.release()

def insert_2(threadName, args):
    for root, dirs, files in os.walk("Data/"+args):
        #print root
        for f in files:
            path = os.path.join(root, f)
            f = open(path, 'r')
            for i in range(6):
                a=f.readline()
            while 1:
                a=f.readline()
                if a=='': break
                substr=a.split(',')

                cur.execute("INSERT INTO test (Uid, Date, Time, Lat, Lon) VALUES (%s, %s, %s, %s, %s)", (args, substr[5], substr[6], substr[0], substr[1]))

threadLock = threading.Lock()
threads = []

# Create new threads
thread1 = myThread_2(1, "Thread-1", "000")
thread2 = myThread_2(2, "Thread-2", "001")
thread3 = myThread_2(3, "Thread-3", "002")
thread4 = myThread_2(4, "Thread-4", "003")
thread5 = myThread_2(5, "Thread-5", "004")
thread6 = myThread_2(6, "Thread-6", "005")
# Start new Threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
# Add threads to thread list
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)
threads.append(thread5)
threads.append(thread6)
# Wait for all threads to complete
for t in threads:
    t.join()
#print "Exiting Main Thread"

tStop = time.time()
print "the insert time with PostgreSQL:" + `(tStop - tStart)`

tStart = time.time()
cur.execute("SELECT * FROM test WHERE Date = '2008-11-19' AND Uid = '003' ORDER BY Time;")
cur.fetchall()

tStop = time.time()
print "the query time with PostgreSQL:" + `(tStop - tStart)`

cur.close()
conn.close()

