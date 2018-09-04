# -*- coding: UTF-8 -*-

import threading
import subprocess
import serial
import thread
import time
import os
import Queue
import logging
import unzipcode
#import nd_data

from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler

## D Frame Example: D11|131933,32109059&132099
#M_frame_msg = ''
def channel1_recv(myser):
    data = '';
    try:
        while True:
            data = myser.readline();
            if data == '':
               continue
            else:
               break
            time.sleep(0.02)
    except KeyboardInterrupt, e:                  
        logger.error('str(KeyboardInterrupt):\t', exc_info=True)
        myser.close();
    return data

class Main_QueueIn_Consumer(threading.Thread):
    def __init__(self,name,queue):
        threading.Thread.__init__(self,name=name)
        self.data=queue
        self.lastts = "";

    def process_data(self):
        global ticket_retry_recv_cnt;
        global recv_pkgs;

        try:     
            val = self.data.get();
        except Queue.Empty:
            time.sleep(0.02);
            logger.debug('empty');
            return;

        logger.debug("Main_QueueIn_Consumer:"+val);

        ## R Frame example: R12|34|14
        ## D Frame example: D315|1234,65&2345,66&5678

        if val[0] == 'R':  # R Frame
            # 解析R帧，将设备id是本机的加入到R_queue_in中
            val = val[1:];
            requests = val.split('|');
            for i in range(0,len(requests)):
                dev = requests[i][0];
                request_id = requests[i][1:];
                if dev == str(dev_index) :
                    if request_id not in R_queue_in_list:
                        logger.debug("REQ_IN:"+request_id);
                        R_queue_in.put(request_id);
                        R_queue_in_list.append(request_id);
        elif val[0]== 'D': # D Frame
            # 收到D Frame之后，publish mqtt消息
            response_id = val.split('|')[0][1:];

            logger.debug("frameid:"+response_id);

          #  logger.debug(str(R_queue_out_list));
          #  logger.debug(str(R_queue_out_wr_map));

            if response_id in recv_pkgs:
                logger.debug("duplicate D frame "+ response_id + ", igone");

                try:
                    if R_queue_out_wr_map.get(response_id) is not None:
                        del R_queue_out_wr_map[response_id];

                    if response_id in R_queue_out_list:
                        R_queue_out_list.remove(response_id);
                except Exception, e:
                    logger.error('remove request queue error', exc_info=True);
                return;

            # 过滤出真正需要的内容
            if  response_id in R_queue_out_list :
                logger.debug("RES_IN:"+response_id);

                recv_pkgs.append(response_id);

                R_queue_out_list.remove(response_id);
                ## 从WR_map中删除指定元素
                if R_queue_out_wr_map.get(response_id) is not None:
                    del R_queue_out_wr_map[response_id];

                val = val.split('|')[1];
                dev_responses = val.split('&');
                for i in range(0,len(dev_responses)):
                    one_response = dev_responses[i];
                    fields = one_response.split(',');
                    ticket_id = unzipcode.code_decode(fields[0]);
                    if len(fields) == 2:
                        ts = unzipcode.time_unzip(fields[1]);
                        self.lastts = ts;
                    else:
                        ts = self.lastts;

                    msg = ticket_id+","+ts+","+response_id[0];
                    cmd = "mosquitto_pub -t sensor_outter_r -q 2 -m \""+ msg + "\"";
                    os.system(cmd);
                    logger.debug(cmd);        
        elif val[0] == 'T':
            if val[1] == '0' or val[1] == str(dev_index) :
                ticket_cnt = val[2:].split("|")[0];
                ticket_interval = val[2:].split("|")[1];

                cmd = "python /root/web/testmqtt.py "+ticket_cnt + " "+ticket_interval + " 20";
                os.system(cmd);
                logger.debug(cmd);     
        elif val[0] == 'M':
            if val[1] == '1': # val[1]为1，表示是设备验票数
                cmd = "mosquitto_pub -t ticket_stats -q 2 -m \""+ val[2:] + "\"";
                os.system(cmd); 
           
    def run(self):
        logger.info("Main_QueueIn_Consumer thread started");
        while True:
            try:
                self.process_data();
            except Exception, e:
                logger.error('Main_QueueIn_Consumer error', exc_info=True);

class Main_RQueueOut_Consumer(threading.Thread):
    def __init__(self,name,queue):
        threading.Thread.__init__(self,name=name)
        self.data=queue
        self.cnt = 0;
        self.msg = "R";
        self.lastts = time.time()

    def process_data(self):
        cur_time = time.time()
        if self.cnt == 4 or (self.cnt > 0 and cur_time - self.lastts >= 4.0):
            logger.debug("Main_RQueueOut_Consumer--Encoded Msg:"+self.msg);

            R_queue_encoded_out.put(self.msg);
            self.cnt = 0;

        try:
            val = self.data.get_nowait();
        except Queue.Empty:
            time.sleep(0.02);
            return;

        val = val.replace("\n","")
        if val == '':
            return;

        logger.debug("Main_RQueueOut_Consumer:"+val);

        if self.cnt == 0:
            self.lastts = int(time.time());
            self.msg = "R";
        else :
            self.msg = self.msg + "|";

        self.msg = self.msg + val;
        self.cnt = self.cnt + 1;

        logger.debug("recv before:"+val);        

    def run(self):
        logger.info("Main_RQueueOut_Consumer thread started");
        while True:
            try:
                self.process_data();
            except Exception, e:
                logger.error('Main_RQueueOut_Consumer error', exc_info=True);


class Main_DQueueOut_Consumer(threading.Thread):
    def __init__(self,name,queue):
        threading.Thread.__init__(self,name=name)
        self.data=queue

    def process_data(self):
        try:
            val = self.data.get();
        except Queue.Empty:
            time.sleep(0.02);
            self.logger.debug('empty');
            return;

        logger.debug("Main_DQueueOut_Consumer:"+val);
        msg = A_queue_map.get(val);

        if msg is not None:
            logger.debug("Main_DQueueOut_Consumer--Encoded Msg:"+msg);
            R_queue_encoded_out.put(msg);
        else:
            logger.debug("Main_DQueueOut_Consumer-- cannot find "+val);        

    def run(self):
        logger.info("Main_DQueueOut_Consumer thread started");
        while True:
            try:
                self.process_data();
            except Exception, e:
                logger.error('Main_DQueueOut_Consumer error', exc_info=True);


def lora_send_consumer():
    global lora_send_timer;
    global main_send_queue;
    global slot_index;
    global R_queue_encoded_out;
    global M_frame_msg

   # logger.debug("[[--Start-- "+str(int(round(time.time() * 1000))));

    lora_send_timer = threading.Timer(lora_interval, lora_send_consumer);
    lora_send_timer.start()

    package_sent = 0;

    slot_index = slot_index + 1;
    if int(dev_index) == 1 and slot_index == beacon_interval: # master
        logger.debug("send BEACON! ts = " + str(int(round(time.time() * 1000))));
        ser_main.write("BE\n");
        package_sent = package_sent + 1;
        slot_index = 0;  
    #logger.debug("send M1! ts = " + str(int(round(time.time() * 1000))));
    ser_main.write("M1"+M_frame_msg+'\n')

    slot_start_ts = int(round(time.time() * 1000));

    while True:

        slot_curtime = int(round(time.time() * 1000));

        if slot_curtime - slot_start_ts > lora_slot * 1000 :
    #        logger.debug("--End--]] slot timeup: start:"+str(slot_start_ts) +",end:" + str(int(round(time.time() * 1000))));
            break;

        if package_sent >= max_package_sent :
    #        logger.debug("--End--]] slot max packages: start:"+str(slot_start_ts) +",end:" + str(int(round(time.time() * 1000))))
            break;

        try:     
            #logger.debug("before dequeue");
            next_job = R_queue_encoded_out.get_nowait();
            logger.debug("after dequeue");
        except Queue.Empty:
            #logger.debug("queue empty delay");
            time.sleep(0.01);
            continue;
        except Exception, e:
            logger.debug("Read queue error");
            logger.error('Read queue error', exc_info=True);
            time.sleep(0.01);
            continue;

        ticket_info = next_job;

        logger.debug("-->Dequeue:" + ticket_info);

        if ticket_info[-1] != '\n' :
            ser_main.write(ticket_info+"\n");
        else :
            ser_main.write(ticket_info);

        ## 将已发送的R帧加入到Wait Response Map中
        if ticket_info.startswith('R'):
            ts = int(time.time());
            requests = ticket_info[1:].split('|')
            logger.debug("add to WR:"+str(requests));
            for i in range(0,len(requests)):
                R_queue_out_wr_map[requests[i]] = ts;
                logger.debug("REQ_OUT"+requests[i]);
            logger.debug("WR_Map:"+str(R_queue_out_wr_map))
        # 将已发送的D帧对应的id从R_queue_in_list中去除
        elif ticket_info.startswith('D'):
            response_id = ticket_info.split('|')[0][2:];
            R_queue_in_list.remove(response_id);
            logger.debug("RES_OUT:"+response_id);

        package_sent = package_sent + 1;

        logger.debug("----->"+str(dev_index) +"|" + str(package_sent) + "| sending lora:" + ticket_info);

def check_WRMap_consumer():
    global wr_interval;
    check_WRMap_timer = threading.Timer(wr_interval, check_WRMap_consumer);
    check_WRMap_timer.start()

    try:

        cur_time = int(time.time());

        logger.debug("wr_queuelen:"+str(R_queue_out.qsize()));
        logger.debug("wrlist_before2"+str(R_queue_out_list));
        logger.debug("wrmap_before3"+str(R_queue_out_wr_map));
        logger.debug("recv_pkgs:"+str(recv_pkgs));

        for key in R_queue_out_wr_map.keys():

            logger.debug("cps:"+key+","+str(cur_time)+","+str(R_queue_out_wr_map[key]));

            if cur_time - R_queue_out_wr_map[key] >= wr_interval :
                ## 将WR_Map中超过15s的Request重新入队列

                logger.debug("cps_lxy:"+key);

                if key in recv_pkgs:
                    logger.debug("cps_remove from list:"+key);
                    if key in R_queue_out_list:
                        R_queue_out_list.remove(key);
                else:
                    R_queue_out.put(key);
                    logger.debug("Re-Request:"+key);

                    if not key in R_queue_out_list:
                        R_queue_out_list.append(key);
                        logger.debug("cps_add to list"+key);

                ## 从map中删除元素
                if R_queue_out_wr_map.get(key) is not None:
                    logger.debug('cps_delete from map'+key);
                    del R_queue_out_wr_map[key]
            else:
                logger.debug("cps_skip:"+key);

        logger.debug("wr_after1"+str(R_queue_out.qsize()));
        logger.debug("wrlist_after2"+str(R_queue_out_list));
        logger.debug("wrmap_after3"+str(R_queue_out_wr_map));

    except Exception, e:
        logger.error('check_WRMap_consumer error', exc_info=True);


class M_Frame_inner_Recv(threading.Thread):
    def __init__(self,name):

        threading.Thread.__init__(self,name=name)

    def run(self):
        global ticket_inner_cnt;
        global DEV_TYPE
        global lora_sensor_inner_record
        if DEV_TYPE == 0:
            order='mosquitto_sub -i mos_retry4 -q 2 -t sensor_inner'
        else:
            order = 'mosquitto_sub -t ticket_total'
        pi= subprocess.Popen(order,shell=True,stdout=subprocess.PIPE)

        for i in iter(pi.stdout.readline,'b'):
            if i == "\n":
                continue;
            if DEV_TYPE == 1:
                logger.debug('--------------->M_Frame_contact:'+i)
                ticket_inner_cnt = int(i)
            else:
                ticket_inner_cnt = ticket_inner_cnt+lora_sensor_inner_record[0] + 1;
                logger.debug('--------------->M_Frame_contact:'+i)


class T_Frame_Recv(threading.Thread):
    def __init__(self,name):

        threading.Thread.__init__(self,name=name)

    def run(self):

        order='mosquitto_sub -q 2 -t sensor_test'

        pi= subprocess.Popen(order,shell=True,stdout=subprocess.PIPE)
        restart_count = 0
        for i in iter(pi.stdout.readline,'b'):
            if i == "\n":
                continue;
            if i == b'' or i == '':
                restart_count = restart_count + 1

            i = i.replace('\n','');

            try:
                logger.debug('--------------->T_Frame_contact:'+i)
                ticket_cnt = i.split("|")[0];
                ticket_interval = i.split("|")[1];
		
                R_queue_encoded_out.put("T0"+ticket_cnt+"|"+ticket_interval);

                cmd = "python /root/web/testmqtt.py "+ticket_cnt + " "+ticket_interval + " 20";
                os.system(cmd);
                logger.debug(cmd);  

            except Exception, e:
                logger.error('T_Frame_Recv', exc_info=True);

class A_queue_out_Recv(threading.Thread):
    def __init__(self,name):

        threading.Thread.__init__(self,name=name)

    def run(self):

        order='mosquitto_sub -q 2 -t sensor_lora'

        pi= subprocess.Popen(order,shell=True,stdout=subprocess.PIPE)
        restart_count = 0
        for i in iter(pi.stdout.readline,'b'):
            if i == "\n":
                continue;
            if i == b'' or i == '':
                restart_count = restart_count + 1

            logger.debug("AQueue Enqueue:" + i);

            A_queue_map[i.split("|")[0][2:]] = i;

def lora_recv(threadName, delay):
    global recv_count;
    while True:
        data = channel1_recv(ser_main)
        data = data.replace("\0","");
        if data != b'' or data != '\n' :
            data = data.replace('\n','');
            logger.debug(data)

        global lora_send_timer;

        if len(data) < 2:
            continue;

        # if received BEACON frame, restart the timer;
        if data == "BE" and int(dev_index) != 1:
            logger.debug('Received BEACON!!!')
            try:
                lora_send_timer.cancel();
                logger.debug('Cancel timer')
            except Exception, e:
                logger.debug('Cancel timer Exception');

            lora_send_timer = threading.Timer(lora_slot_offset, lora_send_consumer);
            lora_send_timer.start();
        elif (data.startswith('D') or data.startswith('R')) or data.startswith('T') or data.startswith('M') :
            recv_count = recv_count + 1;
            logger.debug('Enqueue received lora msg');
            main_recv_queue.put(data);

def start_master():
    lora_send_timer = threading.Timer(0, lora_send_consumer);
    lora_send_timer.start();

def testdata():

    A_queue_map["2"]="D12|Vxa,EJk&Vxb&Vxc"
    A_queue_map["3"]="D13|Vxs,EJk&Vxt&Vxv"
    A_queue_map["4"]="D14|Vxt,EJk&Vxu&Vxv"

    main_queue_out.put("210");
    R_queue_out_list.append("210");

    main_queue_out.put("14");
    R_queue_out_list.append("14");

    main_queue_in.put("D210|Vxs,EJk&Vxs&Vxs")
    main_queue_in.put("D14|Vxs,EJk&Vxt&Vxv")
    main_queue_in.put("R12|34|14")

    R_queue_out.put("1")
    R_queue_out.put("2")
    R_queue_out.put("3")
    R_queue_out.put("4")

    R_queue_out_list.append("1")
    R_queue_out_list.append("2")
    R_queue_out_list.append("3")
    R_queue_out_list.append("4")

def initLog():
    global logger;
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    ts = int(time.time())
    FILE_NAME = 'lora_retry.log'
    handler = RotatingFileHandler(FILE_NAME,maxBytes= 512*1024,backupCount =2)
    # create a file handler
    #handler = logging.FileHandler(str(ts)+'+'+'lora_retry.log')
    handler.setLevel(logging.DEBUG)

    # create a logging format

    formatter = logging.Formatter('%(asctime)s - %(levelname)s %(lineno)s - %(message)s')
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)  

    # add the handlers to the logger

    logger.addHandler(handler)
    logger.addHandler(console_handler)


def WatchDog_consumer():
    watchDog_timer = threading.Timer(dev_count*3, WatchDog_consumer);
    watchDog_timer.start();

    logger.debug("cps: m1");

    global ticket_inner_cnt;
    global M_frame_msg
    global DEV_TYPE
    
    M_frame_msg = str(dev_index) + str(ticket_inner_cnt);
    ##print msg
    logger.debug('------------------>watchdog_cunsumer:'+M_frame_msg)
    #R_queue_encoded_out.put("M1"+msg+'\n');

    if DEV_TYPE == 1:

        cmd = 'mosquitto_pub -q 2 -t sensor_stats -m \"'+M_frame_msg +"\"";
        os.system(cmd);
        logger.debug(cmd); 
    else:
        pass
        
     

if __name__ == "__main__":

    initLog();

    global main_send_queue;
    M_frame_msg = ''
    main_send_queue = Queue.Queue();
    main_recv_queue = Queue.Queue();

    D_queue_encoded_out = Queue.Queue();

    lora_recv_consumer = Main_QueueIn_Consumer('main_recv_consumer',main_recv_queue);
    lora_recv_consumer.start();

    f = open('lora.config','r');
    mystr = f.readline();
    mystr = mystr.replace('\n','');
    configs = mystr.split(',');

    dev_index = int(configs[0]);
    dev_count = int(configs[1]);
    beacon_interval = int(configs[2])
    #DEV_TYPE =  int(configs[3])
    #后期通过读取数据库确定设备ID是DOT还是70,0为70,1为DOT
    DEV_TYPE = 0

    f.close();

    slot_index = 0;
    lora_slot = 0.7;
    lora_slot_offset = 1*(dev_index-1);
    lora_interval = 1*dev_count;
    max_package_sent = 5

    #ser0 = serial.Serial('/dev/cu.Bluetooth-Incoming-Port', 115200, timeout=0.5) 

    ser0 = serial.Serial('/dev/ttyS1', 115200, timeout=0.5) 
    # ser1 = serial.Serial('/dev/ttyS1', 115200, timeout=0.5) 
    ser_main = ser0;

    if ser0.isOpen() :
        logger.info('ttyS1 open success');
    else :
        logger.info('ttyS1 open failed');

    if dev_index == 1:
        start_master();
    conn = nd_data.connect_database('lora_protect.db')
    lora_sensor_inner_record = nd_data.select_database(conn,'LoRa_local_data','lora_sensor_inner')

    main_queue_out = Queue.Queue();
    main_queue_in = Queue.Queue();

    R_queue_out = Queue.Queue();
    R_queue_out_list = [];
    R_queue_encoded_out = Queue.Queue();

    R_queue_out_wr_map = {};

    R_queue_in = Queue.Queue();
    R_queue_in_list = [];

    A_queue_map = {};

    Main_QueueIn_Consumer = Main_QueueIn_Consumer('main_recv_consumer',main_queue_in);
    Main_QueueIn_Consumer.start();

    a_queue_Recv = A_queue_out_Recv('main_aqueue_consumer');
    a_queue_Recv.start();

    main_RQueueOut_Consumer = Main_RQueueOut_Consumer('main_rqueueout_consumer',R_queue_out);
    main_RQueueOut_Consumer.start();

    main_DQueueOut_Consumer = Main_DQueueOut_Consumer('main_dqueueout_consumer',R_queue_in);
    main_DQueueOut_Consumer.start();

    #testdata();

    ser_main = ser0;
    # ser_recv = ser1;

    recv_count = 0;

    ticket_retry_recv_cnt = 0;
    ticket_inner_cnt = 0;

    wr_interval = dev_count * 1;

    recv_missing_queues = Queue.Queue();
    global recv_pkg_indexes;
    recv_pkg_indexes = {};
    recv_pkgs = [];

    try:
        check_WRMap_timer = threading.Timer(0, check_WRMap_consumer);
        check_WRMap_timer.start();

        m_Frame_inner_Recv = M_Frame_inner_Recv('m_frame_inner_recv');
        m_Frame_inner_Recv.start();

        t_Frame_Recv = T_Frame_Recv('t_frame_inner_recv');
        t_Frame_Recv.start();

        watchDog_timer = threading.Timer(0, WatchDog_consumer);
        watchDog_timer.start();
    except Exception, e:
        logger.error('retry_init_error', exc_info=True);


    try:
        thread.start_new_thread( lora_recv, ("Thread-lora", 2, ) )
    except:
        logger.error("Error: unable to start thread");

    order='mosquitto_sub -q 2 -t sensor_retry'

    pi= subprocess.Popen(order,shell=True,stdout=subprocess.PIPE)
    restart_count = 0
    for i in iter(pi.stdout.readline,'b'):
        if i == "\n":
            continue;
        if i == b'' or i == '':
            restart_count = restart_count + 1
            # if restart_count == 10:
            #     logger.debug('restart_lora_retry')
            #     cmd = "/etc/init.d/lora_retry restart"
            #     os.system(cmd)


        logger.debug("R queue out Enqueue:" + i);

        i = i.replace('\n','');
        logger.debug('---------------->retry_data:'+i)	
        R_queue_out.put(i);
        R_queue_out_list.append(i);
