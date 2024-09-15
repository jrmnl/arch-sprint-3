
docker exec -it kafka-1 opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --create --topic device --replication-factor 2 --partitions 3
docker exec -it kafka-1 opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --create --topic telemetry