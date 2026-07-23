## 어플리케이션 시작 전 토픽 생성

1. ticket-reservations, ticket-payments 토픽 생성 (Partitions: 3, Replication Factor: 3)

docker exec -it kafka-1 kafka-topics \
--bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
--create --topic ticket-reservations --partitions 3 --replication-factor 3

docker exec -it kafka-1 kafka-topics \
--bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
--create --topic ticket-payments --partitions 3 --replication-factor 3


## 토픽 생성 확인

1. 생성된 토픽 리스트 확인
docker exec -it kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

2. ticket-reservations 토픽 상세 상태 확인
docker exec -it kafka-1 kafka-topics --bootstrap-server localhost:9092 --topic ticket-reservations --describe

3. ticket-payments 토픽 상세 상태 확인
docker exec -it kafka-1 kafka-topics --bootstrap-server localhost:9092 --topic ticket-payments --describe


## 대용량 테스트
ab -n 10000 -c 100 -p /dev/null -T "application/x-www-form-urlencoded" "http://localhost:8080/reserve?userId=user_test&ticketId=concert_01"


## 적재 테스트
