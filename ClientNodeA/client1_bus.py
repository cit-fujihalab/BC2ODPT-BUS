import signal
from time import sleep
import json
from core.client_core import ClientCore
from p2p.message_manager import MSG_NEW_TRANSACTION
import threading
import time
import subprocess
import gc
import base64
import os

my_p2p_client = None

#SLEEP_TIME
SleepTime = 30

def signal_handler(signal, frame):
    shutdown_client()

def shutdown_client():
    global my_p2p_client
    my_p2p_client.shutdown()

def transaction_send():

    while True:
        try:
            # dirname = os.path.dirname(__file__)
            # cmd = "python3" + dirname +" /APIurllib.py"
            cmd = "python3 APIurllib.py"
            print(cmd)
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            bus_data = proc.stdout.read()
            transaction = json.loads(bus_data)

            for JP in transaction: # 日本語排除
                del JP["odpt:note"]

            print("tp.set_new_transaction(transaction)")
            global my_p2p_client
            my_p2p_client.send_message_to_my_core_node(MSG_NEW_TRANSACTION,json.dumps(transaction, sort_keys=True, ensure_ascii=False))
            

        except:
            print("transaction is empty, skip.")

        sleeptime = SleepTime 
        print('sleeptime is ' + str(sleeptime))
        sleep(sleeptime)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    global my_p2p_client
    #任意のIPに変更
    # my_p2p_client = ClientCore(自端末のポート番号, '接続先のIP7アドレス' ,接続先のポート番号) #　
    # my_p2p_client = ClientCore(50089, '118.243.116.125' ,50085) # 藤原サーバへのアクセス
    my_p2p_client = ClientCore(50089, '10.84.242.68' ,50085) # 
    my_p2p_client.start()
    transaction_send()

if __name__ == '__main__':
    main()
