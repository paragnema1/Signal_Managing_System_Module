
'''Import OCC packages '''
from sms_dlm_conf import *
from sms_log import *
import sys
import json
from peewee import *
from datetime import datetime, timedelta
from sms_dlm_model import *
sys.path.insert(1, "./common")


class SmsApi:
    '''OCC DAtabase operations such as Select, Insert, Delete records'''

    def __init__(self):
        self.train_trace_obj_list = []

    def connect_database(self, config):
        '''Establish connection with database'''
        try:
            self.json_data = config.json_data
            self.db_name = self.json_data["DATABASE"]["DB_NAME"]
            self.user = self.json_data["DATABASE"]["USER"]
            self.password = self.json_data["DATABASE"]["PASSWORD"]
            self.host = self.json_data["DATABASE"]["HOST"]
            self.port = 5432

            if len(self.db_name) == 0:
                Log.logger.critical(
                    "sms_dlm_api: connect_database:  database name missing")
            else:
                psql_db = PostgresqlDatabase(self.db_name, user=self.user, password=self.password,
                                             host=self.host, port=self.port)
                if psql_db:
                    try:
                        psql_db.connect()
                        Log.logger.info(
                            f'sms_dlm_api: database connection successful')
                        return psql_db
                    except Exception as e:
                        Log.logger.critical(
                            f'sms_dlm_api: connect_database: {e}')
                        sys.exit(1)
                else:
                    return None
        except Exception as ex:
            Log.logger.critical(
                f'sms_dlm_api: connect_database: Exception: {ex}')

            
    def insert_signal_playback_info(self, data): #add passed data in SignalPlayback table
        '''insert point status for playback'''
        try:
            json_data = json.loads(data) #convert passed data to python dictionary
            signal_playback_table = SignalPlaybackInfo() #initialize SignalPlaybackInfo table
            signal_playback_table.ts = json_data["ts"] #add timestamp from passed data to table
            signal_playback_table.signal_data = json_data #add passed data to table
            print("------------------------------------------")
            print(json_data) #printing passed data 
            signal_playback_table.save() #saving added data to table
        except Exception as ex:
            Log.logger.critical(
                f'sms_dlm_api : insert_signal_playback_info: {ex}')

    def get_section_id(self, point_id): #return records from pms_config table where point_id is matched with passed point_id.
        '''get section id for the given point id'''
        try:
            point_current_table = SmsConfig.select().where(
                SmsConfig.point_id == point_id).get()
            return point_current_table.section_id
        except DoesNotExist:
            return
        
if __name__ == '__main__':
    print("INITTTT")
    cfg = SmsDlmConfRead()
    cfg.read_cfg('../config/sms.conf')

    sms_api = SmsApi()
    db_conn = sms_api.connect_database(cfg)

    if db_conn:
        print("DATABASE CONNECTED")

    else:
        pass
