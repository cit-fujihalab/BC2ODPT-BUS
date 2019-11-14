import Crypto
import Crypto.Random
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
import binascii
import json
import os

class DigitalSignature:
    # RSAの鍵ペアを生成する
    def __init__(self):
        #self._private_key = RSA.importKey(key_data, pass_phrase)
        #self._pubric_key = self._private_key.publickey()
        #self._signaer = PKCS1_v1_5.new(self._private_key)
        random_gen = Crypto.Random.new().read
        self.private_key = RSA.generate(2048, random_gen)
        self.public_key = self.private_key.publickey()
        if os.path.exists("pubkey.pem"):
            print("前回作成されたKeyを使用します。")
            with open("pubkey.pem", "r") as pubkey:
                data = pubkey.read()
                target = binascii.unhexlify(data)
                # 本来は鍵ペアのファイルが不正などの異常系処理を考えるべき
                self.private_key = RSA.importKey(target)
                self.public_key = self.private_key.publickey()
                print("read_pubkey")
        else :
            print("ファイルが存在しませんので新規でKeyを取得します。")
            with open("pubkey.pem", "w") as pubkey:
                my_pem = self.private_key.exportKey(format="PEM")
                pubkey.write(binascii.hexlify(my_pem).decode("ascii"))
                print("Generate_privatekey")

        print("privatekey") 
        print(self.private_key.exportKey(format="PEM"))
        print("publickey")
        print(self.public_key.exportKey(format="PEM"))

    # 与えられた秘密鍵を使ってメッセージに署名する
    def compute_digital_signature(self, message):
        # まずメッセージをハッシュ値に変換する
        hashed_message = SHA256.new(message.encode('utf8'))
        # 署名にはRSASSA-PKCS1-v1_5を利用する
        signer = PKCS1_v1_5.new(self.private_key)
        print(type(binascii.hexlify(signer.sign(hashed_message)).decode('ascii')))#
        return binascii.hexlify(signer.sign(hashed_message)).decode('ascii')#

    def get_public_key(self):
        return self.public_key

    def get_private_key(self):
        return self.private_key

    def to_str_public_key(self):
         pubkey_bytes = self.public_key.exportKey()
         return pubkey_bytes.decode('ascii')

    def to_RSA_class(self, key_str):
        key_bytes = key_str.encode('utf-8')
        return RSA.importKey(key_bytes)

    def add_public_key(self,msg):
        msg_signature = self.compute_digital_signature(msg)
        list = json.loads(msg)
        list.append({})
        list[len(list)-1]['signature'] = msg_signature
        list[len(list)-1]['address'] = self.to_str_public_key()
        return json.dumps(list, sort_keys=True, ensure_ascii=False)

    def check_signature(self, tr_list):
        sig_data = tr_list[len(tr_list) - 1] #{'signature':value1,'address':value2}
        original_data = tr_list[0:len(tr_list) - 1]
        od_str = json.dumps(original_data, sort_keys=True, ensure_ascii=False)
        hashed_od_str = SHA256.new(od_str.encode('utf8'))
        pub_key = self.to_RSA_class( sig_data.get('address') )
        print('Public_key:',pub_key)
        verifier = PKCS1_v1_5.new( pub_key )
        flag = verifier.verify(hashed_od_str, binascii.unhexlify(sig_data.get('signature')))
        return flag

    def import_key_pair(self):
        self._private_key = RSA.importKey()
        self._pubric_key = self

class CheckDigitalSignature:

    def __init__(self, tr_list):
        sig_data = tr_list[len(tr_list) - 1] #{'signature':value1,'address':value2}
        original_data = tr_list[0:len(tr_list) - 1]
        od_str = json.dumps(original_data, sort_keys=True, ensure_ascii=False)
        hashed_od_str = SHA256.new(od_str.encode('utf8'))
        pub_key = self.to_RSA_class( sig_data.get('address') )
        verifier = PKCS1_v1_5.new( pub_key )
        self.flag = verifier.verify(hashed_od_str, binascii.unhexlify(sig_data.get('signature')))

    def to_RSA_class(self, key_str):
        key_bytes = key_str.encode()
        return RSA.importKey(key_bytes)

    def get_flag(self):
        return self.flag
