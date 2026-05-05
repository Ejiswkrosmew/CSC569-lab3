# Lab 3 - Election/Coordination

Spring 2026

CSC-569-01 Advanced Distributed Systems

James Yaguma

Ethan Emery

## Instructions


This is an implementation of the Paxos consensus algorithm built on top of our membership protocol from lab 2. In order to run the algorithm:
1) go run server.go
2) spin up clients using:
	- go run client.go <client-id-#>
3) Select a client to be a proposer and hit the enter key while that client runs
4) This will initiate paxos and there will be printouts of the status of the algorithm in the terminal
5) The server terminal will show the status of proposals and acceptances

