## Read me

Create topic for consumers 1-4

kafka-topics.sh --create --topic orderprojekt --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Create topic - 

Create topic for consumers 5-6

kafka-topics.sh --create --topic hanteradeorders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Unless you want to rename it yourself in the program, so to speak.


I started the producer as usual in VS Code and used PowerShell/cmd to start all the consumers.

The database is SQLite.

In case you don’t already know, you need to start the producer so that the database is created in order to be able to run consumer 4.
