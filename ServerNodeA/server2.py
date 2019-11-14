import signal

from core.server_core import ServerCore


my_p2p_server = None

def signal_handler(signal, frame):
	shutdown_server()

def shutdown_server():
	global my_p2p_server
	my_p2p_server.shutdown()


def main():
	signal.signal(signal.SIGINT, signal_handler)
	global my_p2p_server
	# my_p2p_server = ServerCore(50089, '10.84.247.69',50085) # note
	# my_p2p_server = ServerCore(50090, '10.84.242.68', 50082) # DTop
	my_p2p_server = ServerCore(50090, '192.168.0.142', 50085)
	my_p2p_server.start()
	my_p2p_server.join_network() 
	# my_p2p_server.start_gui()

if __name__ == '__main__':
	main()
