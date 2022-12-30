# NATS Pipe examples kit

It is a small suite of examples for stream computation with NATS in unix pipes or even as a basement for HADOOP's like computation in the NATS ecosystem. 
Joined logging for errors to some NATS subject and publishing metrics to separated NATS service 


## Get dependencies
go get github.com/nats-io/nats.go

## Single stream echoing

It listen from_subject, print (or transform) input message and pass it to to_subject stream  

Build it:
go build NATS_Pipe_Stream.go

And use like:
NATS_Pipe_Stream from_subject to_subject

Dataflow:
NATS from_subject ===> handler ====> to_subject of NATS

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


## Multiple streams echoing with metrics collection and publishing in some NATS end point

It like abowe but gather input data from many NATS sourcies

Build it:
go build NATS_Pipe_Streams_Log_Metrics.go

Using:
 NATS_Pipe_Streams_Log_Metrics from_subj1 ... from_subj4  to_subject log_subject metrics_subject
 
 Dataflow: NATS from_subject1 ===> Handler ====> to_subject of NATS
                from_subject2 ===>         ====> logger_subject 
                from_subject3 ===>         ====> metrics_subject
                ....
                from_subjectn ===>
