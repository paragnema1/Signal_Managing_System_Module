'''
*****************************************************************************
*File : sms_dlm_model.py
*Module : sms_dlm
*Purpose : Database model class for design postgresql database to store data.
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python module'''
from peewee import *
from datetime import datetime
import sys
from playhouse.postgres_ext import *

'''Import wild module'''
sys.path.insert(1,"./common")
from sms_log import *
from sms_dlm_conf import *

if Log.logger is None:
    my_log = Log()

'''read configuration file'''
cfg = SmsDlmConfRead()
cfg.read_cfg('../config/sms.conf')

json_data = cfg.json_data
db_name = json_data["DATABASE"]["DB_NAME"]
user = json_data["DATABASE"]["USER"]
password = json_data["DATABASE"]["PASSWORD"]
host = json_data["DATABASE"]["HOST"]
port = 5432
psql_db = None

try:  
    psql_db = PostgresqlDatabase(db_name, user= user, password= password, host=host, port=port)
    if psql_db != None:
        psql_db.connect()
except Exception as e:
    Log.logger.critical(f'sms_dlm_model: Exception: {e}')


class SccModel(Model):
    """A base model that will use our Postgresql database"""
    class Meta:
        database = psql_db

class SmsConfig(SccModel):
    section_id = CharField()
    point_id = CharField()
    dp_id = ArrayField(CharField, null = True) 
    class Meta:
        table_name = "pms_config"

class SignalPlaybackInfo(SccModel):
    ''' Signal Playback table '''
    ts = DoubleField()
    signal_data = JSONField()

    class Meta:
        table_name = "signal_playback"





if __name__ == '__main__':
    if Log.logger is None:
      my_log = Log()
    Log.logger.info("sms_model: main program")
    #psql_db.create_tables([PmsConfig, PointInfo, PointCurrentInfo, PointPlaybackInfo, PointUserInfo, ErrorInfo])
    #psql_db.create_tables([PointUserInfo])
