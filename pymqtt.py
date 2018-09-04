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
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler
## D Frame Example: D11|131933,32109059&132099

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

class LoRa_Recv_Consumer(threading.Thread):
    def __init__(self,name,queue):
        threading.Thread.__init__(self,name=name)
        self.data=queue

    def process_data(self):
        try:     
            val = self.data.get();
        except Queue.Empty:
            time.sleep(0.02);
            return;

        logger.debug("recv before:"+val);

        recv_headers = val.split('|');

        if val[0] != 'D' or len(recv_headers) == 1:
            logger.debug("not D Frame,ignore! "+ val[0] +","+str(len(recv_headers)))
            return;

        ### 找到重试的数据包
        recv_dev = recv_headers[0][1];
        recv_pkg_index = int(recv_headers[0][2:]);

        logger.debug("recv_dev:"+recv_dev+",recv_pkg_index:"+str(recv_pkg_index));

        if recv_pkg_indexes.get(recv_dev) is None:
            recv_pkg_indexes[recv_dev] = recv_pkg_index;

        max_index = recv_pkg_indexes[recv_dev];
        if recv_pkg_index > max_index + 1 :
            lost_cnt = recv_pkg_index - max_index;
            for i in range(max_index+1,recv_pkg_index) :
                retry_msg = recv_dev + str(i);
                cmd = "mosquitto_pub -t sensor_retry -q 2 -m "+ retry_msg;
                logger.debug("retry:"+cmd);
                os.system(cmd)

        recv_pkg_indexes[recv_dev] = recv_pkg_index;

        ### 解析数据，发送mqtt消息
        encoded_data = recv_headers[1];

        fields = encoded_data.split("&");
        ts = "";
        tid = "";
        for i in range(0,len(fields)):
            if i == 0:
                tid = fields[0].split(",")[0]
                ts = unzipcode.time_unzip(fields[0].split(",")[1]);
            else:
                tid = fields[i];

            outter_data = unzipcode.code_decode(tid) + "," + ts +","+val[1];
            cmd = "mosquitto_pub -t sensor_outter -q 2 -m \""+ outter_data +"\"" ;
            os.system(cmd)
            logger.debug("sending outter mqtt:"+cmd);                


    def run(self):
        logger.info("LoRa_Recv_Consumer thread started");
        while True:
            try:
                self.process_data();
            except Exception, e:
                logger.error('LoRa_Recv_Consumer error', exc_info=True);

        logger.debug("%s finished!" % self.getName());

def lora_send_consumer():
    global lora_send_timer;
    global main_send_queue;
    global slot_index;
    global D_queue_encoded_out;

    #logger.debug("[[--Start-- "+str(int(round(time.time() * 1000))));

    lora_send_timer = threading.Timer(lora_interval, lora_send_consumer);
    lora_send_timer.start()

    package_sent = 0;

    slot_index = slot_index + 1;
    if int(dev_index) == 1 and slot_index == beacon_interval: # master
        logger.debug("send BEACON! ts = " + str(int(round(time.time() * 1000))));
        ser_main.write("B"+unzipcode.time_zip(str(int(round(time.time())))) +"\n");
        package_sent = package_sent + 1;
        slot_index = 0;    

    slot_start_ts = int(round(time.time() * 1000));

    while True:
        slot_curtime = int(round(time.time() * 1000));

        if slot_curtime - slot_start_ts > lora_slot * 1000 :
      #      logger.debug("--End--]] slot timeup: start:"+str(slot_start_ts) +",end:" + str(int(round(time.time() * 1000))));
            break;

        if package_sent >= max_package_sent :
      #      logger.debug("--End--]] slot max packages: start:"+str(slot_start_ts) +",end:" + str(int(round(time.time() * 1000))))
            break;

        try:
            #logger.debug("before dequeue");
            next_job = D_queue_encoded_out.get_nowait();
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

        package_sent = package_sent + 1;

        logger.debug("----->"+str(dev_index) +"|" + str(package_sent) + "| sending lora:" + ticket_info);

class Main_DQueueOut_Consumer(threading.Thread):
    def __init__(self,name,queue,index):
        threading.Thread.__init__(self,name=name)
        self.data=queue
        self.cnt = 0;
        self.dev_index = index;
        self.lastts = int(time.time())
        self.msg = ""
        self.send_index = 1

        self.msg = "D" + str(self.dev_index) + str(self.send_index) + "|";

    ## D Frame Example: D11|131933,32109059&132099

    def encodeTime(self,ts):
        cur_time = int(time.time())
        hour_diff = cur_time % 3600
        time_base = cur_time - hour_diff;

        return int(ts) - time_base;

    def process_data(self):
        cur_time = int(time.time())
        if self.cnt == 3 or (self.cnt > 0 and cur_time - self.lastts >= 2):
            logger.debug("Main_QueueOut_Consumer--Encoded Msg"+self.msg + ",cur_time"+str(cur_time)+",lastts"+str(self.lastts));

            self.msg = self.msg + "\n";

            D_queue_encoded_out.put(self.msg);

            cmd = "mosquitto_pub -t sensor_lora -q 2 -m \""+ self.msg + "\"";
            logger.debug("lora_send:"+self.msg);
            os.system(cmd)

            self.cnt = 0;
            self.send_index = self.send_index + 1;
            self.msg = "D" + str(self.dev_index) + str(self.send_index) + "|";
            self.lastts = int(time.time())

        try:
            val = self.data.get_nowait();
        except Queue.Empty:
            time.sleep(0.1);
            return;

        ticketid = val.split(',')[0];

        if self.cnt != 0:
            self.msg = self.msg + "&";

        self.msg = self.msg + unzipcode.code_encode(ticketid);

        if self.cnt == 0:
            ts = val.split(',')[1];
            self.lastts = int(time.time())
            self.msg = self.msg + "," + unzipcode.time_zip(ts);      

        self.cnt = self.cnt + 1;

        logger.debug("encoded D Msg:"+self.msg);       

    def run(self):
        logger.info("Main_DQueueOut_Consumer thread started");
        while True:
            try:
                self.process_data();
            except Exception, e:
                logger.error('Main_DQueueOut_Consumer error', exc_info=True);

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
        if data[0] == "B" and int(dev_index) != 1:
            logger.debug('Received BEACON!!!')
            try:
                lora_send_timer.cancel();
                logger.debug('Cancel timer')
                time_diff = int(round(time.time())) - int(unzipcode.time_unzip(data[1:]));
                if time_diff > -60 and time_diff < 60:
                    logger.debug('------> time synch pass <-------')
                    pass;
                else:
                    unzipcode.time_synch(data[1:])
                    logger.debug('----->time synch<-----------')
            except Exception, e:
                logger.debug('Cancel timer Exception');

            lora_send_timer = threading.Timer(lora_slot_offset, lora_send_consumer);
            lora_send_timer.start();
        elif data.startswith('D'):
            # data = data[2:];
            recv_count = recv_count + 1;
            logger.debug('Enqueue received lora_msg');
            main_recv_queue.put(data);

def start_master():
    lora_send_timer = threading.Timer(0, lora_send_consumer);
    lora_send_timer.start();

def initLog():
    global logger;
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    ts = int(time.time())
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # create a file handler
    #handler = logging.FileHandler('lora.log')
    FILE_NAME = 'lora.log'
    handler = RotatingFileHandler(FILE_NAME,maxBytes= 512*1024,backupCount =2)
    handler.setLevel(logging.DEBUG)

    # create a logging format

    formatter = logging.Formatter('%(asctime)s - %(levelname)s %(lineno)s - %(message)s')
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)  

    # add the handlers to the logger

    logger.addHandler(handler)
    logger.addHandler(console_handler)

def recv_debug():
    main_recv_queue.put("D2110|2U+j,84b&1mR1")
 #   main_recv_queue.put("D210|Vxs,EJk&Vxs&Vxs")
 #   main_recv_queue.put("D213|Vxs,EJk&Vxs&Vxs")
 #   main_recv_queue.put("D22|1Kh7,6Gf")

if __name__ == "__main__":

    initLog();

    global main_send_queue;
    main_send_queue = Queue.Queue();
    main_recv_queue = Queue.Queue();

    D_queue_encoded_out = Queue.Queue();

    lora_recv_consumer = LoRa_Recv_Consumer('main_recv_consumer',main_recv_queue);
    lora_recv_consumer.start();

    f = open('lora.config','r');
    mystr = f.readline();
    mystr = mystr.replace('\n','');
    configs = mystr.split(',');

    dev_index = int(configs[0]);
    dev_count = int(configs[1]);
    beacon_interval = int(configs[2])

    f.close();

    slot_index = 0;
    lora_slot = 0.35;
    lora_slot_offset = 0.7*(dev_index-1);
    lora_interval = 0.7*dev_count
    max_package_sent = 3

    #ser0 = serial.Serial('/dev/cu.Bluetooth-Incoming-Port', 115200, timeout=0.5) 

    ser0 = serial.Serial('/dev/ttyS0', 115200, timeout=0.5) 
    # ser1 = serial.Serial('/dev/ttyS1', 115200, timeout=0.5) 

    if ser0.isOpen() :
        logger.info('ttyS0 open success');
    else :
        logger.info('ttyS0 open failed');

    # if ser1.isOpen() :
    #     logger.info("ttyS1 open success")
    # else :
    #     logger.info("open failed")

    if dev_index == 1:
        start_master();

    D_queue_encoded_out = Queue.Queue();
    main_Dqueueout_consumer = Main_DQueueOut_Consumer('main_dqueueout_consumer',main_send_queue,dev_index);
    main_Dqueueout_consumer.start();

    #recv_debug();

    ser_main = ser0;
    # ser_recv = ser1;

    recv_count = 0;

    recv_missing_queues = Queue.Queue();
    global recv_pkg_indexes;
    recv_pkg_indexes = {};

    try:
        thread.start_new_thread( lora_recv, ("Thread-lora", 2, ) )
    except:
        logger.error("Error: unable to start thread");

    order='mosquitto_sub  -t sensor_inner'

    pi= subprocess.Popen(order,shell=True,stdout=subprocess.PIPE)

    for i in iter(pi.stdout.readline,'b'):
        if i == "\n" :
            continue;

        logger.debug("sensor_inner:" + i);

        main_send_queue.put(i);
        

