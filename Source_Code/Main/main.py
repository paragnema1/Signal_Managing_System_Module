from sms_dlm_conf import *
from sms_dlm_model import *
from sms_dlm_api import *
from common.mqtt_client import *
from common.sms_log import *
import sys
import json
import threading
import signal
import time

'''Import custom packges '''
sys.path.insert(1, "./common")
            
class SoftSgnalServer:
    def __init__(self, mqtt_client):
        self.mqtt_client = mqtt_client
        self.sms_api = SmsApi()
        self.signal_info = []
        self.reset_signal={}
        self.section_info = {}
        self.cwsm_pub_point_request={}
        self.signal_json_data = {}
        self.point_status_list=[]
        self.signal_set_status = []
        self.point_current_status = []
    
    def read_cfg(self, file_name): #Convert file into python whose path is passed and return it.
        try:
            if path.exists(file_name):
                Log.logger.info('does not have valid Json Config\  Program terminated')
                with open(file_name) as f:
                        self.signal_json_data = json.load(f)
                        return self.signal_json_data
            else:
                Log.logger.critical(
                    f'{file_name} not found.  Program terminated')
                sys.exit(1)
        except Exception as ex:
            Log.logger.critical(f'read_cfg: exception {ex}')

    #UPER 2
    def sem_section_info_sub_fn(self,in_client, user_data, message): #function remove those section_id from passed data whose section_status is occupied.
        '''occ section information'''
        try:
            self.json_section_info = json.loads(message.payload) #converting passed message in json to pyhton.
            for signal in self.signal_info:
                if len(signal["sections"]) == 0: #if section of any signal is empty then publish signal id and remove the signal from self.signal_info list.
                    cancelsignalPublishMsg = {"cancel_signal_id" : signal['signal_id']}
                    json_sms_signal_reset_info_msg = json.dumps(cancelsignalPublishMsg)
                    self.mqtt_client.pub("sms/signal_info", json_sms_signal_reset_info_msg)
                    self.signal_info.remove(signal) #removes the signal from self.signal_info.
                    
                for section_id in signal["sections"]:
                    section_data = list(filter(lambda d: d["section_id"] == section_id.upper(),self.json_section_info["sections"])) #filtering from passed data whose section_id matches with section_id of signal_info (an object of read_cfg function).
                    if section_data[0]["section_status"] == "occupied":  #if in filtered secion_id section_status is occupied then remove that section_id from signal_info.
                        clearsignalPublishMsg = json.dumps({"clear_signal_id" : signal['signal_id']})
                        self.mqtt_client.pub("sms/signal_info", clearsignalPublishMsg)
                        signal["sections"].remove(section_id)

        except Exception as ex:
            Log.logger.critical(f'sem_section_info_sub_fn : exception: {ex}')
    
    #UPER        
    def signal_msg_sub_fn(self,in_client, user_data, message): #function calling "process_cwsm_msg function" and pass this passed message as an attribute to this function.
        '''cwsm/signal_control subscribe function to handle signal control request'''
        try:
            cwsm_point_control_msg = json.loads(message.payload) #convert passed message in json to python.
            self.process_cwsm_msg(cwsm_point_control_msg) #call process_cwsm_msg function.
        except Exception as ex:
            Log.logger.critical(f'cwsm_signal_control_sub_fn: exception {ex}')

    def point_info_sub_fn(self, in_client, user_data, message): #function updating [signal_status of signal_info list after checking some conditions from signal_json_data] & [point_status of 'points' list if point_id in message is also present in self.point_current_status]
        #In detail description : if point_id in message is also present in self.point_current_status then update point_status of 'points' list if point_id in message is also present in self.point_current_status.
        #for signal_id in signal_json_data also present in signal_info dictionary, and signal_status of these signals is "progress", and section_mark_id is "mark_selected" , and "point_current_status" of all point_machines is True, then update signal_status of these signals to "selected" and add point_ids to signal_info dictionary.
        
        '''subscribe pms/point_info receive from pms'''
        try:
            msg_payload = json.loads(message.payload) #convert passed message to python.
            pointInfo = { "point_id":msg_payload["point_id"], "direction":msg_payload["point_status"] } #making pointinfo message taking point_id and point_status from passed message.
            # setting the current status
            points = list(filter(lambda d: d["point_id"] == pointInfo["point_id"], self.point_current_status)) #making list of those points whose point_id matches with point_id of passed message.
            if points: #if points list is not empty or not null.
                points[0]["direction"] = pointInfo["direction"] #updating/adding points[0]direction from pointInfo direction.
            else:
                self.point_current_status.append(pointInfo) #appending pointInfo to point_current_status list.


            signal_list = self.signal_json_data['routing_data'] #takes routing_data from signal_json_data.(an object of read_cfg function)
            for signal in self.signal_info: #iterating through each signal in signal_info list.
                signal_data = list(filter(lambda d: d["signal_id"] == signal["signal_id"], signal_list)) #making list of those signals in signal_list whose signal_id matches with signal_id of signal_info list.
                if  signal['signal_status'] == "progress": #if signal_status in signal_info list is progress.
                    signal_data=list(filter(lambda d: d["section_mark_id"] == signal["mark_selected"], signal_data[0]['section_marks'])) #filtering those signals from signal_data[0]['section_marks'] whose section_mark_id matches with signal["mark_selected"] in signal_info list.
                    if "point_machines" in signal_data[0]:
                        if [d in self.point_current_status for d in signal_data[0]["point_machines"]].count(False) == 0: #checking if all the elements in the list signal_data[0]["point_machines"] are present and have a True value in the list self.point_current_status.
                            signal['signal_status'] = "selected" #making signal_status in signal_info list as selected.    
                            signal['point_ids'] = signal_data[0]['point_machines'] #adding point_ids to signal_info list.
                    else:
                        signal['signal_status'] = "selected"    
                        self.signal_set_status=[]     #clearing the signal_set_status list.
        except Exception as ex:
           Log.logger.critical(f'point_info_sub_fn: exception: {ex}')
    
    #UPER 3
    def check_section_status(self, point_id): #function returns section_status of section whose point_id is passed.
        try:
            section_point_id = "none"
            section_point_status = "none"

            '''get section_id for the given point_id'''
            section_point_id = self.sms_api.get_section_id(point_id)
            if bool(self.json_section_info) == True: #if json_section_info is not empty or not null.
                for section in self.json_section_info["sections"]: #iterating through each section in json_section_info and if section_id of section matches with section_id of passed point_id then it returns section_status of that section.
                    if section_point_id == section["section_id"]:
                        section_point_status = section["section_status"]
            return section_point_status

        except Exception as ex:
            Log.logger.critical('check_section_status: exception:',ex)

    #UPER 5
    def validate_point_info(self, message): #publish passed message to mqtt topic "cwsm/point_control" only iff section_status of section whose point_id is passed is cleared.
        try:
            curr_section_point_status = self.check_section_status(message["point_id"]) #function retuns section_status of section where passed point_id is present.
            if curr_section_point_status == "cleared": 
                self.cwsm_pub_point_request = message
                self.publish_To_PMS()
        except Exception as ex:
            Log.logger.critical("EXCEPTION ::",ex)

    #NEECHE 0
    def publish_To_PMS(self): #function to publish point request to mqtt.
        Log.logger.info("### PUBLISHING TO PMS ####")
        cwsm_pub_active_signal = json.dumps(self.cwsm_pub_point_request) #convert 'cwsm_pub_point_request' from python to json.
        self.mqtt_client.pub("cwsm/point_control", cwsm_pub_active_signal) #publishing converted 'cwsm_pub_point_request' to mqtt.

    #NEECHE 1
    def process_cwsm_msg(self, cwsm_signal_control_msg):
        #'''if signal_status in passed message is set, then add cwsm_signal_control_msg to 'insert signal playback info' and validate pms_lock_message and pms_point_change_message.'''
        #'''if signal_status in passed message is cancel signal, then publish 'cancelsignalPublishMsg' and remove this signal from signal_info list.'''
        
        try:
            if cwsm_signal_control_msg['signal_status'] == "set": 
                
                '''add cwsm_signal_control_msg to 'insert signal playback info' table after converting it into json.'''
                json_cwsm_signal_control_msg=json.dumps(cwsm_signal_control_msg)
                self.sms_api.insert_signal_playback_info(json_cwsm_signal_control_msg)
                
                signalPublishMsg = {"signal_id":cwsm_signal_control_msg['signal_id'],"sections":cwsm_signal_control_msg['sections'],"sections_controlled":cwsm_signal_control_msg['sections_controlled'],"signal_controlled":cwsm_signal_control_msg['signal_controlled'],"mark_selected":cwsm_signal_control_msg['mark_selected'],"signal_status":"progress" , "username": cwsm_signal_control_msg['username'],'user_ip':  cwsm_signal_control_msg['user_ip']}
                
                '''if signalPublishMsg is not in self.signal_info then append signalPublishMsg to self.signal_info.'''
                if signalPublishMsg not in self.signal_info:
                    self.signal_info.append(signalPublishMsg)
                
                
                signal_list = self.signal_json_data['routing_data'] #takes routing_data from signal_json_data.(an object of read_cfg function)
                signal_data = list(filter(lambda d: d["signal_id"] == cwsm_signal_control_msg["signal_id"], signal_list)) #filter values from data in read_cfg fucntion whose signal_id matches with signal_id of passed data.
                signal_data = list(filter(lambda d: d["section_mark_id"] == cwsm_signal_control_msg["mark_selected"], signal_data[0]['section_marks'])) #filter values from list signal_data[0]['section_marks'] whose section_mark_id matches with section_mark of passed data 
                for point in signal_data[0]['point_machines']: #for each point in point_machines of signal_data[0] validating pms_lock message and pms_point_change message.
                        pms_lock_message = {"ts": time.time(), "point_id":point['point_id'] , "point_operate": "lock", "username": cwsm_signal_control_msg['username'],'user_ip':  cwsm_signal_control_msg['user_ip']}
                        self.validate_point_info(pms_lock_message)
                        pms_point_change_message = {"ts": time.time(), "point_id":point['point_id'] , "point_operate": point['direction'], "username": cwsm_signal_control_msg['username'],'user_ip':  cwsm_signal_control_msg['user_ip']}
                        self.validate_point_info(pms_point_change_message)

            if  cwsm_signal_control_msg['signal_status'] == 'cancel_signal': #if singal status in passed data is cancel_signal.
                
                '''publishing cancelsignalPublishMsg''' 
                cancelsignalPublishMsg = {"cancel_signal_id" : cwsm_signal_control_msg['signal_id']}
                json_sms_signal_reset_info_msg = json.dumps(cancelsignalPublishMsg)
                self.mqtt_client.pub("sms/signal_info", json_sms_signal_reset_info_msg)
                
                '''remove signal from self.signal_info list if signal_id of signal matches with signal_id of passed data.'''
                for signal in self.signal_info:
                    if signal['signal_id'] == cwsm_signal_control_msg['signal_id'] :
                        if cwsm_signal_control_msg['signal_status'] == 'cancel_signal':
                            if "point_id" in signal:
                                point_id = signal['point_id']
                                pms_unlock_message = {"ts": time.time(), "point_id":point_id , "point_operate": "unlock", "username": cwsm_signal_control_msg['username'],'user_ip':  cwsm_signal_control_msg['user_ip']}
                                self.validate_point_info(pms_unlock_message)
                                self.signal_info.remove(signal)
                            else:
                                self.signal_info.remove(signal)

        except Exception as ex:
            Log.logger.critical(f'publish_signal_info: exception {ex}')

    #UPER 4
    def publish_signal_info(self): #function to publish singal_info list as a message to topic sms/signal_info every 1 sec.
        try:
            Log.logger.info("#########################################  ACTIVE SIGNALS   ###################################################")
            while True:
                json_sms_signal_info_msg = json.dumps(self.signal_info)
                self.mqtt_client.pub("sms/signal_info", json_sms_signal_info_msg)
                time.sleep(1)
        except Exception as ex:
            Log.logger.critical(f'error in publishing: exception {ex}')

if __name__ == "__main__":
    '''Read Database Configuration'''
    if Log.logger is None:
        my_log = Log()    
    cfg = SmsDlmConfRead()
    cfg.read_cfg('../config/sms.conf')
    '''start MQTT client connection'''
    try:

        mqtt_client = MqttClient(cfg.lmb['BROKER_IP_ADDRESS'], cfg.lmb['PORT'], cfg.sms_id, cfg.lmb['USERNAME'], cfg.lmb['PASSWORD'],
                                    cfg.sms_id)
        mqtt_client.connect()
    except Exception as ex:
        Log.logger.critical(f'mqtt exception: {ex}')

    sms_server = SoftSgnalServer(mqtt_client)

    signal_cfg = sms_server.read_cfg('../config/signal.conf')

    '''subscribe sem/section_info mqtt topic'''
    mqtt_client.sub("sem/section_info", sms_server.sem_section_info_sub_fn)

    '''subscribe cwsm/signal_control mqtt topic'''
    mqtt_client.sub("cwsm/signal_control", sms_server.signal_msg_sub_fn)
    mqtt_client.sub("pms/point_info",sms_server.point_info_sub_fn)

    time.sleep(0.001)

    t1 = threading.Thread(target=sms_server.publish_signal_info,
                            args=(), name='CwsmMsgTh', daemon=True)
    t1.start()

    t2 = threading.Thread(target=sms_server.publish_To_PMS,
                            args=(), name='CwsmMsgTh', daemon=True)
    t2.start()

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    threading.Event().wait()