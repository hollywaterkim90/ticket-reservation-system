## 어플리케이션 시작 전 토픽 생성

1. ticket-reservations, ticket-payments 토픽 생성 (Partitions: 3, Replication Factor: 3)

docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --topic ticket-reservations --partitions 3 --replication-factor 3
docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --topic ticket-payments --partitions 3 --replication-factor 3


## 토픽 생성 확인

1. 생성된 토픽 리스트 확인
docker exec -it kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

2. ticket-reservations 토픽 상세 상태 확인
docker exec -it kafka-1 kafka-topics --bootstrap-server localhost:9092 --topic ticket-reservations --describe

3. ticket-payments 토픽 상세 상태 확인
docker exec -it kafka-1 kafka-topics --bootstrap-server localhost:9092 --topic ticket-payments --describe


## 대용량 테스트

# user1부터 user100까지 100건의 요청을 백그라운드(&)로 동시에 발사
for i in {1..100}; do
curl -X POST "http://localhost:8080/reserve" \
-d "userId=user$i&ticketId=ticket01" &
done
wait


## 적재 테스트
