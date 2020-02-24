import logging
import os
from time import time
from datetime import datetime
import pandas as pd
from fbprophet import Prophet
from redistimeseries.client import Client
def predict(host,port,t0):
    connection=Client(host=host,port=port)
    data=connection.range("Temperature",
                from_time=0,
                to_time=-1,
                bucket_size_msec=60*5, # In Seconds NOT milliseconds
                aggregation_type='avg')
    time,value=zip(*data)
    time=[datetime.fromtimestamp(x) for x in time]
    df=pd.DataFrame(dict(ds=time,y=value))
    m = Prophet(changepoint_prior_scale=0.02,interval_width=.95).fit(df)
    future = m.make_future_dataframe(periods=6, freq='H')
    fcst = m.predict(future)
    fcst=fcst.set_index('ds')
    out=fcst[['yhat','yhat_upper','yhat_lower']]
    logging.warning(out)
