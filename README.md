# NATS Pipe examples kit

It is a small suite of examples for stream computation with NATS in unix pipes or even as a basement for HADOOP's like computation in the NATS ecosystem 



## Get dependencies
go get github.com/nats-io/nats.go

## Single stream echoing

It listen from_subject, print (or transform) input message and pass it to to_subject stream  

Build it:
go build NATS_Pipe_Stream.go

And use like:
NATS_Pipe_Stream from_subject to_subject

from_subject === handler ====> to_subject

It listen from_subject, print (or transform) input message and pass it to to_subject stream  

## Single stream echoing whith logging

It like NATS_Pipe_Stream but with logging events and errors to some NATS destination point

Build it:
go build NATS_Pipe_Stream_Log.go

Using:
 NATS_Pipe_Stream_Log from_subject to_subject log_subject

## Multiple streams echoing

It like abowe but gather input data from many NATS sourcies

Build it:
go build NATS_Pipe_Streams_Log.go

Using:
 NATS_Pipe_Streams_Log from_subj1 ... from_subj4  to_subject log_subject
