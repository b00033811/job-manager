from time import time,sleep,mktime
from datetime import datetime
from sys import exit
import os
from rq import Queue
import logging
import confuse
from redistimeseries.client import Client
logging.basicConfig(format='%(asctime)s %(message)s',
 datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.DEBUG)
class jobManager():
    def __init__(self):
        self.MSEC=1
        self.SEC=self.MSEC*1000
        self.MIN=60*self.SEC
        self.HOUR=60*self.MIN
        self.DAY=24*self.HOUR
        self.new_records=[]
        self.stream_pointer=[]
        self.start_time=[]
        self.q=[]
        cwd = os.getcwd()
        logging.warning('Current Working Directory: {0}'.format(cwd))
        try:
            if 'JOBMANAGER' in os.environ:
                self.appname=os.environ['JOBMANAGER']
                logging.info('Job manager name: '+ self.appname)
                self.config=confuse.LazyConfig(self.appname)
                if self.config.keys()==[]:
                    logging.warn('No keys detected, please ensure that a configmap is configured.')
                    self._debug_config()    
            else:
                raise ValueError('Enviroment variable not set: JOBMANAGER') 
        except ValueError as e:
            exit(e)
        filename=self.config['STREAM_LOG_DIR'].get()
        if os.path.exists(filename):
            logging.warning('Log File Detected!')
        # Intialize Redis Time Series Client
        self.rts,self.q=self._init_rts()
        self.frequency=self.config['FREQUENCY'].get()
    def _init_rts(self):
        try:
            #rts = Client(host=os.environ.get('REDIS_HOST'),port=os.environ.get('REDIS_PORT'))
            self.host=self.config['REDIS_HOST'].get()
            self.port=self.config['REDIS_PORT'].get()
            rts = Client(host=self.host,port=self.port,decode_responses=True)
            q=Queue(connection=rts)
        except:
            logging.warning('Failed To initialize Redis client')
            logging.warning(self.config.keys())
            raise
        else:
            logging.warning('Redis Client Initialized')
        finally:
            return rts,q
    def _debug_config(self):
        directory=self.config.config_dir()
        con_file_dir=self.config.user_config_path()
        logging.warning('Config file Directory: {0} | User Config File:{1}'.format(directory,con_file_dir))
    def start(self,start_time):
        self.start_time=self.config['START_TIME'].get()
        self.stream=self.config['STREAM'].get()
        self.stream_log_filename=self.config['STREAM_LOG_DIR'].get()
        self.proc_log_filename=self.config['PROC_LOG_DIR'].get()
        if os.path.exists(self.stream_log_filename) & os.path.exists(self.proc_log_filename):
            with open(self.stream_log_filename,'r') as stream_log,open(self.proc_log_filename,'r') as proc_log:
                # read and update stream pointer and record and read stream
                self.stream_pointer=stream_log.readlines()[-1]
                self.new_records=self.rts.xread({self.stream: self.stream_pointer},100,1*self.SEC)
                if self.new_records!=[]:
                    # grab proc record and pointer
                    self.proc_pointer=proc_log.readlines()[-1]
                    self.proc_record=self.rts.xrange(self.stream,min=self.proc_pointer,max=self.proc_pointer,count=1)
                    self.proc_record=self.proc_record[0]
                    self.stream_record=self.new_records[0][1][-1]
                    self.stream_pointer=self.stream_record[0]
                    # log how many new records were read from the stream
                    n_records=len(self.new_records[0][1])
                    # save the stream pointer
                    with open(self.stream_log_filename, 'a') as log:
                        log.write(os.linesep+self.stream_pointer)
                        logging.warn('Records added: {0}'.format(n_records))
                    # invoke the job creation
                    self.create(self.frequency)      
                else:
                    logging.warning('No New Records')

        else:
            logging.warning('Could not locate log files, scaning all available \
                data in the stream and deploying redis jobs from the beginning of the stream')
            # os.makedirs(os.path.dirname(filename), exist_ok=True)
            # consume the stream from the beggining
            self.new_records=self.rts.xread({self.stream: '0-0'},15,0)
            #### drop them into the redis time sereis!##################
            # grab the data from the latest record
            self.stream_record=self.new_records[0][1][-1]
            # grab the data from the earliest record
            self.proc_record=self.new_records[0][1][0]
            # create a pointer for the end of the stream
            self.stream_pointer=self.stream_record[0]
            #create a pointer for the jobs at the beggining of the stream
            self.proc_pointer=self.proc_record[0]
            # open the log file
            with open(self.stream_log_filename,'w') as stream_log,open(self.proc_log_filename,'w') as proc_log:
                # save the location of the pointer processing
                stream_log.write(self.stream_pointer)
                proc_log.write(self.proc_pointer)
            # invoke the job creation function
            self.create(self.frequency)
    def create(self,k):
        # open the processing log 
        with open(self.proc_log_filename,'a') as proc_log:
            # iterate through the records and create a job based on k
            for i,x in enumerate(self.new_records[0][1]):
                t=int(x[1]['time'])
                t0=int(self.proc_record[1]['time'])
                if (t-t0)>=k:
                    self.deploy_job(self.start_time,t,t0)
                    # update the location of the processing pointer 
                    self.proc_record=self.new_records[0][1][i]
                    self.proc_pointer=self.proc_record[0]
                    proc_log.write(os.linesep+self.proc_pointer)
    def deploy_job(self,target_time,t,t0):
        ''' logic: you can stop jobs from being deployed during fail event by supply the required start time'''
        if t0>=target_time:
            job = self.q.enqueue('prophet_algorithm.job.predict',args= (self.host,self.port,t0))
            logging.warning('JOB DEPLOYED tdelta={0}, event time start: {1}'.format(t-t0,t0))
            logging.warning(job)
if __name__== "__main__":
    client=jobManager()
    while(True):
        client.start(1581233435)
