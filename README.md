# Lab 4 - MapReduce

Spring 2026

CSC-569-01 Advanced Distributed Systems

James Yaguma

Ethan Emery

## Instructions

1) Start the server by running "go run server.go"
2) In seperate terminal windows, run up to 8 clients with "go run client.go <client id>"
3) Watch the leader terminal for logged updates on system state
4) when all reduce tasks are complete, as seen on the leader, MapReduce has finished
5) Run the following to view the top 20 words and their frequencies: "cat mr-out-* | sort -n -k 2 -r | head -n 20"
