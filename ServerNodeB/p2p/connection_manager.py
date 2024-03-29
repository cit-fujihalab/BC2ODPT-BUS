import socket
import threading
import pickle
import codecs
import json
import binascii
import os

from concurrent.futures import ThreadPoolExecutor
from websocket_server import WebsocketServer
from websocket import create_connection
import time

from .core_node_list import CoreNodeList
from .edge_node_list import EdgeNodeList
from .message_manager import (
	MessageManager,
	MSG_ADD,
	MSG_REMOVE,
	MSG_CORE_LIST,
	MSG_REQUEST_CORE_LIST,
	MSG_PING,
	MSG_ADD_AS_EDGE,
	MSG_REMOVE_EDGE,
	ERR_PROTOCOL_UNMATCH,
	ERR_VERSION_UNMATCH,
	OK_WITH_PAYLOAD,
	OK_WITHOUT_PAYLOAD,
    Sync_DB3,
    Sync_DB4,
    Sync_DB5,
    Sync_DB6,
    Sync_DB7,
)
from time import sleep

from LDB import get_level_dir, level_param, check_level_all

# 動作確認用の値。本来は30分(1800)くらいがいいのでは
PING_INTERVAL = 10
TIME_OUT = 60

dirname = os.path.dirname(__file__).replace("p2p", "")
LDB_P = dirname+"LDB/DB/ldb/"
PARAM_P = dirname+"LDB/DB/"
ZIP_P = dirname+"LDB/DB/zip/"

RE_LDB_P = dirname+"LDB/DB/ldb/"
RE_PARAM_P = dirname+"LDB/DB/"
RE_ZIP_P = dirname+"LDB/DB/zip/"

class ConnectionManager:

	def __init__(self, host,  my_port, callback, sc_self=None, cb = None, cb2 = None ):
		print('Initializing ConnectionManager...')#####
		self.cb = cb#
		self.cb2 = cb2
		self.host = host
		self.port = my_port
		self.my_c_host = None
		self.my_c_port = None
		self.core_node_set = CoreNodeList()
		self.edge_node_set = EdgeNodeList()
		self.last_ping = {}	##
		self.__add_peer((host, my_port))
		self.mm = MessageManager()
		self.callback = callback
		self.my_host = self.__get_myip()##
		self.ws = WebsocketServer(port = my_port, host = self.my_host)##
		
		self.sc_self = sc_self ##
		self.flag = 0 ##


	# 待受を開始する際に呼び出される（ServerCore向け
	def start(self):
		# t = threading.Thread(target=self.__wait_for_access)
		# t.start()

		self.ping_timer_p = threading.Timer(PING_INTERVAL, self.__check_peers_connection)
		self.ping_timer_p.start()

		self.ping_timer_e = threading.Timer(PING_INTERVAL, self.__check_edges_connection)
		self.ping_timer_e.start()

		self.ws.set_fn_new_client(self.__new_client)
		self.ws.set_fn_message_received(self.__ws_handle)

		t = threading.Thread(target=self.ws.run_forever)
		t.start()

	def __ws_handle(self, client, server, message):
		self.__handle_message((client, client['address'], message), server)
		return
	
	def __new_client(self, client, server):
		print("%s is connected" %(client))
	
	
	# ユーザが指定した既知のCoreノードへの接続（ServerCore向け
	def join_network(self, host, port):
		self.my_c_host = host
		self.my_c_port = port
		self.__connect_to_P2PNW(host, port)


	def get_message_text(self, msg_type, payload = None):
		"""
		指定したメッセージ種別のプロトコルメッセージを作成して返却する

		params:
			msg_type : 作成したいメッセージの種別をMessageManagerの規定に従い指定
			payload : メッセージにデータを格納したい場合に指定する

		return:
			msgtxt : MessageManagerのbuild_messageによって生成されたJSON形式のメッセージ
		"""
		msgtxt = self.mm.build(msg_type, self.port, payload)
		print('generated_msg:', msgtxt  + str("省略中"))

		return msgtxt


	# 指定されたノードに対してメッセージを送信する
	def send_msg(self, peer, msg):
		try:
			ws4edge = create_connection("ws://" + str(peer[0]) + ":" + str(peer[1]))
			ws4edge.send(msg.encode())
			ws4edge.close()

		except OSError:
			print('Connection failed for peer : ', peer)
			self.__remove_peer(peer)

	# Coreノードリストに登録されている全てのノードに対して同じメッセージをブロードキャストする
	def send_msg_to_all_peer(self, msg):
		print('send_msg_to_all_peer was called!')
		current_list = self.core_node_set.get_list()
		for peer in current_list:
			if peer != (self.host, self.port):
				print("message will be sent to ... ", peer)
				self.send_msg(peer, msg)


	# Edgeノードリストに登録されている全てのノードに対して同じメッセージをブロードキャストする
	def send_msg_to_all_edge(self, msg):
		print('send_msg_to_all_edge was called! ')
		current_list = self.edge_node_set.get_list()
		for edge in current_list:
			print("message will be sent to ... " ,edge)
			self.send_msg(edge, msg)


	# 終了前の処理としてソケットを閉じる
	def connection_close(self):
		pass##
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((self.host, self.port))
		self.socket.close()
		s.close()
		self.ping_timer_p.cancel()
		self.ping_timer_e.cancel()
		#離脱要求の送信
		if self.my_c_host is not None:
			msg = self.mm.build(MSG_REMOVE, self.port)
			self.send_msg((self.my_c_host, self.my_c_port), msg)


	def __connect_to_P2PNW(self, host, port):
		msg = self.mm.build(MSG_ADD, self.port)
		self.send_msg((host, port), msg)


	def __wait_for_access(self):
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.bind((self.host, self.port))
		self.socket.listen(0)

		executor = ThreadPoolExecutor(max_workers=10)

		while True:

			print('Waiting for the connection ...')
			soc, addr = self.socket.accept()
			print('Connected by .. ', addr)
			data_sum = ''

			params = (soc, addr, data_sum)
			executor.submit(self.__handle_message, params)

	def __is_in_core_set(self, peer):
		"""
		与えられたnodeがCoreノードのリストに含まれているか？をチェックする

			param:
				peer : IPアドレスとポート番号のタプル
			return:
				True or False
		"""
		return self.core_node_set.has_this_peer(peer)

	def __is_in_edge_set(self, peer):

		return self.edge_node_set.has_this_edge(peer)

	# 受信したメッセージを確認して、内容に応じた処理を行う。クラスの外からは利用しない想定
	def __handle_message(self, params ,server):##

		soc, addr, data_sum = params

		"""
		while True:
			data = soc.recv(1024)
			data_sum = data_sum + data.decode('utf-8')

			if not data:
				break

		if not data_sum:
			return
		"""
		##Parse
		result, reason, cmd, peer_port, payload = self.mm.parse(data_sum)
		print("result, reason, cmd, peer_port, payload")

		status = (result, reason)

		if status == ('error', ERR_PROTOCOL_UNMATCH):
			print('Error: Protocol name is not matched')
			return
		elif status == ('error', ERR_VERSION_UNMATCH):
			print('Error: Protocol version is not matched')
			return

		elif status == ('ok', OK_WITHOUT_PAYLOAD):
			if cmd == MSG_ADD:
				print('ADD node request was received!!')
				self.__add_peer((addr[0], peer_port))
				if(addr[0], peer_port) == (self.host, self.port):
					return
				else:
					cl = pickle.dumps(self.core_node_set.get_list(), 0).decode()
					msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
					self.send_msg_to_all_peer(msg)
					self.send_msg_to_all_edge(msg)

			elif cmd == MSG_REMOVE:
				print('REMOVE request was received!! from', addr[0], peer_port)
				self.__remove_peer((addr[0], peer_port))
				cl = pickle.dumps(self.core_node_set.get_list(), 0).decode()
				msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
				self.send_msg_to_all_peer(msg)
				self.send_msg_to_all_edge(msg)

			##----PING
			elif cmd == MSG_PING:##
				# 特にやること思いつかない
				peer = (addr[0], peer_port)
				if ( self.__is_in_edge_set(peer) ):
					self.edge_node_set.ping_recv(peer)
				print('----------------PING receive!!------------')
				#print('List for Core nodes was requested!!')
				cl = pickle.dumps(self.core_node_set.get_list(), 0).decode()
				msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
				# server.send_message(soc, msg)##
				server.send_message(soc, msg.encode('utf-8'))##
				print("core node list sent")
				return

			elif cmd == MSG_REQUEST_CORE_LIST:
				print('List for Core nodes was requested!!')
				cl = pickle.dumps(self.core_node_set.get_list(), 0).decode()
				msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
				self.send_msg((addr[0], peer_port), msg)

			elif cmd == MSG_ADD_AS_EDGE:
				edge = (addr[0], peer_port)
				self.__add_edge_node(edge)
				cl = pickle.dumps(self.core_node_set.get_list(), 0).decode()
				msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
				self.send_msg((addr[0], peer_port), msg)
				self.last_ping[edge] = time.time()##

			elif cmd == MSG_REMOVE_EDGE:
				print('REMOVE_EDGE request was received!! from', addr[0], peer_port)
				self.__remove_edge_node((addr[0], peer_port))

			
			elif cmd == Sync_DB3:
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■ Sync_DB3 handle ■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				cl = str(get_level_dir.get_late_dir_num(zip_p=ZIP_P))
				msg = self.mm.build(Sync_DB4, self.port, cl)
				self.send_msg((addr[0], peer_port), msg)
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")

			elif cmd == Sync_DB4:
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■ Sync_DB4 handle ■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				new_node_dir = int(get_level_dir.get_late_dir_num(zip_p=RE_ZIP_P))
				latest_dir = int(json.loads(data_sum)["payload"])
				if new_node_dir < latest_dir:
					cl = new_node_dir + 1
					self.flag = 0
					msg = self.mm.build(Sync_DB5, self.port, cl)
					self.send_msg((addr[0], peer_port), msg)
				else:
					if self.flag == 0:
						get_level_dir.unfold_zip_dir(ldb_p=RE_LDB_P, zip_p=RE_ZIP_P)
						level_param.update_key(RE_PARAM_P, level_param.latest_block_num(RE_LDB_P))
						latest_db_bc = check_level_all.valid_all(RE_LDB_P)
					else:
						latest_db_bc = check_level_all.valid_all(RE_LDB_P)
					
					if latest_db_bc:
						print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
						print("DB Valid Check OK !!!!")
						print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
						if self.flag == 0:
							print("■■■■■■■■ Start Sync  ■■■■■■■■■")
							self.sc_self.get_all_chains_for_resolve_conflict()
							self.flag = 1
							
							if "genesis_block" in self.sc_self.bm.chain[0]:
								msg = self.mm.build(Sync_DB5, self.port, str(latest_dir))
								self.send_msg((addr[0], peer_port), msg)
							elif not check_level_all.is_valid_chain([latest_db_bc[0], self.sc_self.bm.chain[0]]):
								msg = self.mm.build(Sync_DB5, self.port, str(latest_dir))
								self.send_msg((addr[0], peer_port), msg)
							else:
								if check_level_all.is_valid_chain([latest_db_bc[0], self.sc_self.bm.chain[0]]):
									print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
									print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
									print("DB2Memory Valid Check OK !!!!")
									print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
									print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
									msg = self.mm.build(Sync_DB7, self.port)
									self.send_msg((addr[0], peer_port), msg)
					else:
						print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
						print("■■■■■■■■ WARNING ■■■■■■■■■ WARNING ■■■■■■■■ WARNING ■■■■■■■■■")
						print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")

			elif cmd == Sync_DB5:
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■ Sync_DB5 handle ■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				receive_dir_num = str(json.loads(data_sum)["payload"])
				re_dir = receive_dir_num.zfill(6)
				p = ZIP_P + "block{}.zip".format(re_dir)
				with open(p, mode="rb") as z:
					z_file = z.read()
				cl = z_file.hex()
				msg = self.mm.build(Sync_DB6, self.port, cl)
				self.send_msg((addr[0], peer_port), msg)
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")

			elif cmd == Sync_DB6:
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■ Sync_DB6 handle ■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				payload = json.loads(data_sum)["payload"]
				latest_dir = int(get_level_dir.get_late_dir_num(zip_p=RE_ZIP_P)) + 1
				p = RE_ZIP_P + "block{}.zip".format(str(latest_dir).zfill(6))
				with open(p, mode="wb") as rz:
					rz.write(binascii.unhexlify(payload))
				msg = self.mm.build(Sync_DB3, self.port)
				self.send_msg((addr[0], peer_port), msg)
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")

			elif cmd == Sync_DB7:
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■ Sync_DB7 handle ■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
				print("■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")

			else:
				is_core = self.__is_in_core_set((addr[0], peer_port))
				self.callback((result, reason, cmd, peer_port, payload), is_core, (addr[0], peer_port))
				return


		elif status == ('ok', OK_WITH_PAYLOAD):
			if cmd == MSG_CORE_LIST:
					# TODO: 受信したリストをただ上書きしてしまうのは本来セキュリティ的には宜しくない。
					# 信頼できるノードの鍵とかをセットしとく必要があるかも
					# このあたりの議論については６章にて補足予定
					print('Refresh the core node list...')
					new_core_set = pickle.loads(payload.encode('utf8'))
					print('latest core node list: ', new_core_set)
					self.core_node_set.overwrite(new_core_set)
					# self.cb(self.core_node_set.get_list())
			else:
				is_core = self.__is_in_core_set((addr[0], peer_port))
				self.callback((result, reason, cmd, peer_port, payload), is_core, None)
				return
		else:
			print('Unexpected status', status)

	def __add_peer(self, peer):
		"""
		Coreノードをリストに追加する。クラスの外からは利用しない想定

		param:
			peer : Coreノードとして格納されるノードの接続情報（IPアドレスとポート番号）
		"""
		self.core_node_set.add((peer))
		try:
			self.cb(self.core_node_set.get_list())
		except:
			pass

	def __add_edge_node(self, edge):
		"""
		Edgeノードをリストに追加する。クラスの外からは利用しない想定

		param:
			edge : Edgeノードとして格納されるノードの接続情報（IPアドレスとポート番号）
		"""
		self.edge_node_set.add((edge))

		try:
			self.cb2(self.edge_node_set.get_list())
		except:
			pass

	def __remove_peer(self, peer):
		"""
		離脱したと判断されるCoreノードをリストから削除する。クラスの外からは利用しない想定

		param:
			peer : 削除するノードの接続先情報（IPアドレスとポート番号）
		"""
		self.core_node_set.remove(peer)

	def __remove_edge_node(self, edge):
		"""
		離脱したと判断されるEdgeノードをリストから削除する。クラスの外からは利用しない想定

		param:
			edge : 削除するノードの接続先情報（IPアドレスとポート番号）
		"""
		self.edge_node_set.remove(edge)
		try:
			self.cb2(self.edge_node_set.get_list())
		except:
			pass

	def __check_peers_connection(self):
		"""
		接続されているCoreノード全ての生存確認を行う。クラスの外からは利用しない想定
		この確認処理は定期的に実行される
		"""
		print('check_peers_connection was called')
		current_core_list = self.core_node_set.get_list()
		changed = False
		dead_c_node_set = list(filter(lambda p: not self.__is_alive(p), current_core_list))
		# self.cb(self.core_node_set.get_list())
		if dead_c_node_set:
			changed = True
			print('Removing peer', dead_c_node_set)
			current_core_list = current_core_list - set(dead_c_node_set)
			# self.core_node_set.overwrite(current_core_list)


		current_core_list = self.core_node_set.get_list()
		print('current core node list:', current_core_list)
		# 変更があった時だけブロードキャストで通知する
		if changed:
			cl = pickle.dumps(current_core_list, 0).decode()
			msg = self.mm.build(MSG_CORE_LIST, self.port, cl)
			self.send_msg_to_all_peer(msg)
			self.send_msg_to_all_edge(msg)
			#self.cb(self.core_node_set.get_list())
		self.ping_timer_p = threading.Timer(PING_INTERVAL, self.__check_peers_connection)
		self.ping_timer_p.start()

	def __check_edges_connection(self):
		"""
		接続されているEdgeノード全ての生存確認を行う。クラスの外からは利用しない想定
		この確認処理は定期的に実行される
		"""

		
		print('check_edges_connection was called')
		current_edge_list = self.edge_node_set.get_list()

		for edge in current_edge_list:##
			if(time.time() - self.edge_node_set.last_ping(edge) > TIME_OUT):##
				self.__remove_edge_node(edge)##
				print("--------edge node " + str(edge) + "Time Out-------------")##

		current_edge_list = self.edge_node_set.get_list()
		print('current edge node list:', current_edge_list)
		self.ping_timer_e = threading.Timer(PING_INTERVAL, self.__check_edges_connection)
		self.ping_timer_e.start()
		try:
			self.cb2(self.edge_node_set.get_list())
		except:
			pass


	def __is_alive(self, target):
		"""
		有効ノード確認メッセージの送信

		param:
			target : 有効ノード確認メッセージの送り先となるノードの接続情報（IPアドレスとポート番号）
		"""
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((target))
			msg_type = MSG_PING
			msg = self.mm.build(msg_type)
			s.sendall(msg.encode('utf-8'))
			s.close()
			return True
		except OSError:
			return False

	def __get_myip(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		s.connect(('8.8.8.8', 80))
		return s.getsockname()[0]

