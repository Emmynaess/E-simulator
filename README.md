## Read me

Skapa topic för consumer 1-4
- kafka-topics.sh --create --topic orderprojekt --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Skapa topic - VG delen (consumer 5-6)
- kafka-topics.sh --create --hanteradeorders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

- Om inte du vill byta namn själv i programmet då so to speak.

Jag startade producer som vanligt i vs code,
och använde mig utav powershell/cmd för att starta alla konsumenter.

## Databas är sqlite
In case of men det vet du säkert att du måste köra igång producern så att databasen skapas för att kunna köra konsument 4.