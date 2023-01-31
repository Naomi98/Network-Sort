# **Project 1 : Distributed Sort**

## Distributed sort specification
In this project, we will have multiple servers instead of one server as in project 0. Each server has its own single input file. At the end of the netsort process, each server will have a portion of the sorted output dataset. The distributed sorting program will concurrently run on all servers. In particular, this netsort program has the ability to sort a data set that is larger than any one server can hold.

In the end, each server will produce a sorted output file with only records belonging to (based on the data partition algorithm below) that server.  If one were to concatenate the output files from server 0 then server 1 etc then there would be a single big sorted file.

## Project objective
You are to write a distributed sorting program that reads in an input file and produces a sorted output file with only records belonging to that server.

