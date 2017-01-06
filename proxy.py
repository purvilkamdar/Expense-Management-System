import socket
import select
import time
import sys
import cb
from time import sleep
import redis


r = redis.Redis(host='localhost', port=6379, db=0)

r.rpush("hosts", "127.0.0.1:5000")
r.rpush("hosts", "127.0.0.1:5001")
r.rpush("hosts", "127.0.0.1:5002")

buffer_size = 4096
delay = 0.0001
forward_to = ('localhost', 5000)
fc = 0
thresh = 3
class Forward:
    def __init__(self):
        self.forward = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self, host, port):
        while True:
		try:
		    self.forward.connect((host, int(port)))
		    return self.forward
		
		except Exception, e:
		    print "Exception caught in connecting to to server %s" %e
                    success, failure, pass_through_failure = run()
		    server.main_loop()
                    

class TheServer(object):
    input_list = [1]
    channel = {}
    def __init__(self, host, port):
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((host, port))
            self.server.listen(200)

    def main_loop(self):
        self.input_list.append(self.server)
        while 1:
            time.sleep(delay)	   
            ss = select.select
            inputready, outputready, exceptready = ss(self.input_list, [], [])
            for self.s in inputready:
		print self.s
                if self.s == self.server:
                    self.on_accept()
                    break

                self.data = self.s.recv(buffer_size)
                if len(self.data) == 0:
                    self.on_close()
                    break
                else:
                    self.on_recv()

    def on_accept(self):
        host = ''
        port = '' 
	fc = 0
	if fc < thresh:	
		host , port = fetch_server()
		forward = Forward().start(host, port)
		clientsock, clientaddr = self.server.accept()		
		if forward:
			print clientaddr, "has connected"
			self.input_list.append(clientsock)
			self.input_list.append(forward)
			self.channel[clientsock] = forward
			self.channel[forward] = clientsock
		else:
			print "Can't establish connection with remote server.",
			print "Closing connection with client side", clientaddr
			clientsock.close()
			fc += 1
	else: 
            	
            	raise Exception(err)
                

		

    def on_close(self):
        print self.s.getpeername(), "has disconnected"
        self.input_list.remove(self.s)
        self.input_list.remove(self.channel[self.s])
        out = self.channel[self.s]
        self.channel[out].close() 
        self.channel[self.s].close()
        del self.channel[out]
        del self.channel[self.s]

    def on_recv(self):
        data = self.data
	print "in on_recv"        
        self.channel[self.s].send(data)

def fetch_server():
	host = ''
	port = '' 
	r.rpoplpush("hosts","hosts")
	server1=r.lindex("hosts",0)
	try:	
		if server1:
			print "connecting to server: %s " %server1
			host, port=server1.split(":")
		else:
			print "No more server in db!!"
	except Exception, e:
		print "e"
	return host, port


MY_EXCEPTION = 'Threw Dependency Exception'

@CircuitBreaker(max_failure_to_open=3, reset_timeout=10)
def dependency_call(call_num):
   pass

def run():
    num_success = 0
    num_failure = 0
    num_circuitbreaker_passthrough_failure = 0
    host = ''
    port = '' 
    for i in range(1,5):
        try:
            print "Call-%d:" % i
            print "Result=%s" %dependency_call(i)
            num_success += 1
        except Exception as ex:
            num_failure += 1
            if ex.message == MY_EXCEPTION:
                num_circuitbreaker_passthrough_failure += 1
            print ex.message

        sleep(0.5)
    return num_success, num_failure, num_circuitbreaker_passthrough_failure

if __name__ == '__main__':
        server = TheServer('127.0.0.1', 9090)
        try:
            server.main_loop()
        except KeyboardInterrupt:
            print "Ctrl C - Stopping server"
sys.exit(1)
