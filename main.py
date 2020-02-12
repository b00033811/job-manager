''' Logic:
1. create a log if it doesnt exist
2. check stream for 7 days worth of data 
3. if there is data start a job with the corrosponding id
4. save job details (last processed sample) 
5. schedual a job in the next 5 minutes 
'''
from time import time,sleep,mktime
from datetime import datetime
from sys import exit
import os
import logging
import confuse
from redistimeseries.client import Client
logging.basicConfig(format='%(asctime)s %(message)s',
 datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.DEBUG)
class jobManager():
    def __init__(self):
        self.frequency=60*15
        self.MSEC=1
        self.SEC=self.MSEC*1000
        self.MIN=60*self.SEC
        self.HOUR=60*self.MIN
        self.DAY=24*self.HOUR
        cwd = os.getcwd()
        logging.warn('Current Working Directory: {0}'.format(cwd))
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
            logging.warn('Log File Detected!')
        # Intialize Redis Time Series Client
        self.rts=self._init_rts()

    def _init_rts(self):
        try:
            #rts = Client(host=os.environ.get('REDIS_HOST'),port=os.environ.get('REDIS_PORT'))
            host=self.config['REDIS_HOST'].get()
            port=self.config['REDIS_PORT'].get()
            rts = Client(host=host,port=port,
            decode_responses=True)
        except:
            logging.warning('Failed To initialize Redis client')
        else:
            logging.warning('Redis Client Initialized')
        finally:
            return rts
    def _debug_config(self):
        directory=self.config.config_dir()
        con_file_dir=self.config.user_config_path()
        logging.warn('Config file Directory: {0} | User Config File:{1}'.format(directory,con_file_dir))
    def pull(self):
        self.stream=self.config['STREAM'].get()
        self.stream_log_filename=self.config['STREAM_LOG_DIR'].get()
        self.proc_log_filename=self.config['PROC_LOG_DIR'].get()
        if os.path.exists(self.stream_log_filename) & os.path.exists(self.proc_log_filename):
            with open(self.stream_log_filename,'r') as log:
                # open log file and grab the id of the last messege consumed
                ids=log.readlines()
                last_id = ids[-1]
                # query and grab the last timestamp and parse it
                new_records=self.rts.xread({self.stream: last_id},15,1*self.SEC)
                if new_records!=[]:
                    n_records=len(new_records[0][1])
                    last_id=new_records[0][1][-1][0]
                    with open(self.stream_log_filename, 'a') as log:
                        log.write(os.linesep+last_id)
                        logging.warn('Records added: {0}'.format(n_records))
                else:
                    logging.warn('No New Records')

        else:
            logging.warn('Could not locate log files, scaning all available \
                data in the stream and deploying redis jobs from the beginning of the stream')
            # os.makedirs(os.path.dirname(filename), exist_ok=True)
            self.new_records=self.rts.xread({self.stream: '0-0'},None,0)
            self.stream_record=self.new_records[0][1][-1]
            self.proc_record=self.new_records[0][1][0]
            self.stream_pointer=self.stream_record[0]
            self.proc_pointer=self.proc_record[0]
            with open(self.stream_log_filename,'w') as stream_log,open(self.proc_log_filename,'w') as proc_log:
                stream_log.write(self.stream_pointer)
                proc_log.write(self.proc_pointer)
            
            self.create(self.frequency)

    def create(self,k):
        with open(self.proc_log_filename,'a') as proc_log:
            for i,x in enumerate(self.new_records[0][1]):
                t=int(x[1]['time'])
                t0=int(self.proc_record[1]['time'])
                if (t-t0)>=k:
                    print('JOB DEPLOYED tdelta={0}, event time start: {1}, Last Consumed Event Time: {2}'.format(t-t0,t0,self.stream_record[1]['time']))
                    self.proc_record=self.new_records[0][1][i]
                    self.proc_pointer=self.proc_record[0]
                    proc_log.write(os.linesep+self.proc_pointer)

if __name__== "__main__":
    consumer=jobManager()
    while(True):
        consumer.pull()