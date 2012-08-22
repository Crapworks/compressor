compressor
==========

in-memory bzip2 compression server

compressor.py
This programm listens on an tcp port for incoming traffic. If new
TCP session is established, a new thread will be spawned. Everything
that is sent on this session will be bzip2-compressed in-memory. If
the client closes the connection, the fully compressed data is sent
via TCP to another server to save the data to disk.

Main intend:
compressor servers: needs only strong cpu and enough memory, no disk space. scalable!
storage: needs only lot of space. also scalable!
