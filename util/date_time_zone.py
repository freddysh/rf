import time, datetime, pytz
from time import strftime, time


def get_date_time():
    tz = pytz.timezone('America/Lima')
    ct = datetime.datetime.now(tz=tz)
    date = ct.strftime("%Y-%m-%d %H:%M:%S")
    
    return str(date)
    
def get_date():
    tz = pytz.timezone('Etc/GMT+5')
    ct = datetime.datetime.now(tz=tz)
    date = ct.strftime("%Y%m%d")
    
    return str(date)
    