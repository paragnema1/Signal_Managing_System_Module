'''import python packages'''
import json
import sys
from os import path
from typing import NamedTuple

import json_checker
from json_checker import Checker

'''Import WILD packages '''
sys.path.insert(1, "./common")
from sms_log import *
from mqtt_client import *

class SmsDlmConfRead:
    schema = {
        "COMMENT": str,
        "VERSION": str,
        "SMS_ID": str,
        "DATABASE": {
                    "PROVIDER": str,
                    "USER": str,
                    "PASSWORD": str,
                    "HOST": str,
                    "DB_NAME": str
                    },
        "LOCAL_MQTT_BROKER":{
                    "BROKER_IP_ADDRESS" : str,
                    "USERNAME" : str,
                    "PASSWORD" : str,
                    "PORT": int
        }
    }

    def __init__(self):
        self.comment = None
        self.version = None
        self.database = None
        self.json_data = None
        self.lmb = None
        self.sms_id = None
    

    def read_cfg(self, file_name):
        if path.exists(file_name):
            with open(file_name) as f:
                try:
                    self.json_data = json.load(f)
                    Log.logger.info(f'Configuration File: {file_name} loaded successfully\n {self.json_data}')
                except json.JSONDecodeError as jex:
                    Log.logger.critical(f'{file_name} does not have valid Json Config\n{jex}\n  Program terminated')
                    sys.exit(2)
        else:
            Log.logger.critical(f'{file_name} not found.  Program terminated')
            sys.exit(1)
        try:
            checker = Checker(SmsDlmConfRead.schema)
            result = checker.validate(self.json_data)
            Log.logger.info(f'{file_name} Checked OK. Result: {result}')
        except json_checker.core.exceptions.DictCheckerError as err:
            Log.logger.critical(f'{file_name} is not valid {err}')
            sys.exit(3)
        try:
            self.comment = self.json_data['COMMENT']
            self.version = self.json_data['VERSION']
            self.lmb = self.json_data['LOCAL_MQTT_BROKER']
            self.sms_id = self.json_data['SMS_ID']
            self.database = DatabaseStruct(**self.json_data['DATABASE'])
            '''validate empty string''' 
            self.validate_cfg()
            Log.logger.info(f'Configuration File: {file_name} Read successfully\n')
        except KeyError as jex:
            Log.logger.critical(f'{file_name} do not have the data: {jex}')
            sys.exit(3)

    def validate_cfg(self):
        try:
            if(not (self.json_data['COMMENT'] and not self.json_data['COMMENT'].isspace())):
                Log.logger.critical(f'Invalid COMMENT')
                sys.exit(4)

            if(not (self.json_data['VERSION'] and not self.json_data['VERSION'].isspace())):
                Log.logger.critical(f'Invalid VERSION')
                sys.exit(4)
            
            if(not (self.json_data['SMS_ID'] and not self.json_data['SMS_ID'].isspace())):
                Log.logger.critical(f'Invalid VERSION')
                sys.exit(4)
            
            lmb = self.json_data['LOCAL_MQTT_BROKER']

            if(not (lmb['BROKER_IP_ADDRESS'] and not lmb['BROKER_IP_ADDRESS'].isspace())):
                Log.logger.critical(f'Invalid BROKER_IP_ADDRESS')
                sys.exit(4)

            sms_db = self.json_data['DATABASE']

            if(not (sms_db['PROVIDER'] and not sms_db['PROVIDER'].isspace())):
                Log.logger.critical(f'Invalid DATABASE PROVIDER')
                sys.exit(4)

            if(not (sms_db['USER'] and not sms_db['USER'].isspace())):
                Log.logger.critical(f'Invalid DATABASE USER')
                sys.exit(4)

            if(not (sms_db['PASSWORD'] and not sms_db['PASSWORD'].isspace())):
                Log.logger.critical(f'Invalid DATABASE PASSWORD')
                sys.exit(4)

            if(not (sms_db['HOST'] and not sms_db['HOST'].isspace())):
                Log.logger.critical(f'Invalid DATABASE HOST')
                sys.exit(4)

            if(not (sms_db['DB_NAME'] and not sms_db['DB_NAME'].isspace())):
                Log.logger.critical(f'Invalid DATABASE DB_NAME')
                sys.exit(4)

        except Exception as ex:
            Log.logger.critical(f'sms_dlm_conf: validate_cfg: exception: {ex}')

class DatabaseStruct(NamedTuple):
    PROVIDER: str
    USER: str
    PASSWORD: str
    HOST: str
    DB_NAME: str

if __name__ == "__main__":
    if Log.logger is None:
      my_log = Log()

    cfg = SmsDlmConfRead()
    
    cfg.read_cfg('../config/sms.conf')
    Log.logger.info('******************  In Main Program *******************')
    Log.logger.info(f'DATABASE: {cfg.database} \n')
