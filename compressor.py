#!/usr/bin/python
# -*- coding: UTF-8 -*-


"""
compressor.py
This programm listens on an tcp port for incoming traffic. If new
TCP session is established, a new thread will be spawned. Everything
that is sent on this session will be bzip2-compressed in-memory. If
the client closes the connection, the fully compressed data is sent
via TCP to another server to save the data to disk.

Main intend: 
compressor servers: needs only strong cpu and enough memory, no disk space. scalable!
storage: needs only lot of space. also scalable!
"""


import argparse
import socket
from threading import Thread
from select import select
from bz2 import BZ2Compressor

class TCPClient(object):
    """
    TCPClient class: connects to a tcp server and communicates via tcp protocol
    @host: host / ip address to connect to
    @prot: remote tcp port to connect to
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def recv(self, bytes=1024, timeout=None):
        """
        recv: read n bytes from socket. optional timeout
        @bytes: max bytes to read from the socket
        @timeout: timeout for the read procedure in seconds
        """

        if timeout:
            readable, writable, errored = select([self.socket, ], [], [], timeout)
            if self.socket in readable:
                return self.socket.recv(bytes)
            else:
                return None
        else:
            return self.socket.recv(bytes)

    def send(self, data, timeout=None):
        """
        send: write data to the socket. optional timeout
        @data: data (str) to send to the socket
        @timeout: timeout for the send procedure in seconds
        """

        if timeout:
            readable, writable, errored = select([self.socket, ], [], [], timeout)
            if self.socket in writable:
                return self.socket.send(data)
            else:
                return None
        else:
            return self.socket.send(data)

    def close(self):
        self.socket.close()


class TCPServerChild(Thread):
    """
    TCPServerChild: class for use with TCPServer. Uses the Thread subclass.
    @socket: client tuple in the form of (socket, client_addr)
    @run: this function should be overwritten with client-handling code
    """

    def __init__(self, socket):
        Thread.__init__(self)

        self.socket, self.address = socket

    def recv(self, bytes=1024, timeout=None):
        """
        recv: read n bytes from socket. optional timeout
        @bytes: max bytes to read from the socket
        @timeout: timeout for the read procedure in seconds
        """

        if timeout:
            readable, writable, errored = select([self.socket, ], [], [], timeout)
            if self.socket in readable:
                return self.socket.recv(bytes)
            if self.socket in errored:
                raise socket.error
            else:
                return None
        else:
            return self.socket.recv(bytes)

    def send(self, data, timeout=None):
        """
        send: write data to the socket. optional timeout
        @data: data (str) to send to the socket
        @timeout: timeout for the send procedure in seconds
        """

        if timeout:
            readable, writable, errored = select([self.socket, ], [], [], timeout)
            if self.socket in writable:
                return self.socket.send(data)
            else:
                return None
        else:
            return self.socket.send(data)

    def run(self):
        """ overwrite with initial server function """

        self.socket.close()


class TCPCompressionServer(TCPServerChild):
    """ Compress incoming data with BZ2 """

    def run(self):
        self.compressor = BZ2Compressor(self.compress)
        self.client = TCPClient(self.remoteip, self.remoteport)

        while True:
            try:
                data = self.recv()
            except:
                break
            else:
                if data:
                    self.compressor.compress(data)
                else:
                    break

        self.socket.close()
        self.client.send(self.compressor.flush())
        self.client.close()
        #open('/tmp/foo.bz2','w+').write(self.compressor.flush())


class TCPServer(object):
    """
    TCPServer class: creates a listening socket and spawns threads for every connected client
    @bind_socket: socket tuple in the form (ipaddr, socket) to bind to
    @callback: class with subclass of TCPServerChild to handle each client connection
    @max_connections: maximum number of concurrend client connections
    """

    thread_pool = []

    def __init__(self, bind_socket, callback, max_connections = 10):
        self.bind_socket = bind_socket
        self.max_connections = max_connections
        self.callback = callback

    def serve_forever(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(self.bind_socket)
        self.sock.listen(self.max_connections)

        while True:
            readable, writable, errored = select([self.sock, ], [], [], 1)
            if self.sock in readable:
                self.thread_pool.append(self.callback(self.sock.accept()))
                self.thread_pool[-1].daemon=True
                self.thread_pool[-1].start()

            # clean up
            for thread in self.thread_pool[:]:
                if not thread.is_alive():
                    self.thread_pool.remove(thread)

            #print len(self.thread_pool)


class StreamCompressor(object):
    """
    StreamCompressor - Receive data over tcp on compress
    them on the fly.

    @lhost: ip address to bind on
    @lport: port to listen on
    @rhost: remote ip (the compressed data is sent there)
    @rport: remote port (the compressed data is sent there)
    @compression: BZIP2 compression level
    """

    def __init__(self, rhost, rport, lhost="0.0.0.0", lport=31337, compression=9):
        self.host = lhost
        self.port = lport

        TCPCompressionServer.compress = compression
        TCPCompressionServer.remoteip = rhost
        TCPCompressionServer.remoteport = rport

    def serve_forever(self):
        self.server = TCPServer((self.host, self.port), TCPCompressionServer)
        self.server.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--bindip", help="bind to a specific ip address", default="0.0.0.0")
    parser.add_argument("-l", "--localport", help="listen on this port for incoming data", default=31337, type=int)
    parser.add_argument("-r", "--remoteip", help="ip where to send the compressed data", required=True)
    parser.add_argument("-p", "--remoteport", help="port where to send the compressed data", required=True, type=int)
    parser.add_argument("-c", "--compress", help="bzip2 compression level", default=9, type=int)
    args = parser.parse_args()

    sc = StreamCompressor(rhost=args.remoteip, rport=args.remoteport, lhost=args.bindip, lport=args.localport, compression=args.compress)
    sc.serve_forever()
