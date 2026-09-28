-- 예매 스트림 1분 윈도우 집계 (Flink SQL 첫 슬라이스, 이슈 #41)
--
-- 실행:
--   docker compose up -d kafka-1 kafka-2 kafka-3 init-kafka flink-jobmanager flink-taskmanager
--   docker exec -it flink-jobmanager ./bin/sql-client.sh   # 붙여넣기
--   또는  docker exec flink-jobmanager ./bin/sql-client.sh -f /tmp/agg.sql
--
-- ⚠️ 아래 정의는 SQL 클라이언트를 끄면 사라진다(메모리 카탈로그). 그래서 이 파일이 원본이다.

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.local-time-zone' = 'Asia/Seoul';   -- 없으면 UTC 로 찍혀 한국 시각보다 9시간 빠르게 보인다

-- 유휴 파티션 대책.
-- 워터마크는 모든 파티션 중 가장 느린 것을 따라간다. 저부하 구간에서는 스티키 파티셔너가
-- 한 파티션에만 메시지를 몰아 넣어 나머지 파티션이 조용해지고, 그 조용한 파티션 때문에
-- 워터마크가 멈춰 윈도우가 영원히 안 닫힌다. "5초간 조용한 파티션은 계산에서 빼라".
SET 'table.exec.source.idle-timeout' = '5s';

CREATE TABLE reservations (
  orderId  STRING,
  status   STRING,
  userId   STRING,
  ticketId STRING,
  -- DTO 에 시각 필드가 없다(orderId/status/userId/ticketId 뿐).
  -- 그래서 카프카 레코드에 브로커가 찍은 타임스탬프를 메타데이터로 끌어다 이벤트 타임으로 쓴다.
  -- 엄밀히는 '예매를 누른 시각'이 아니라 '카프카에 도착한 시각'이다. 제대로 하려면 DTO 에 reservedAt 을 서버가 찍어 넣어야 한다.
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  -- 5초까지 늦게 오는 메시지는 기다려 준다.
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ticket-reservations',
  -- 컨테이너 안에서 도니까 INTERNAL 리스너 주소. localhost:9092 를 쓰면 자기 자신을 가리킨다.
  'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
  'properties.group.id' = 'flink-agg-demo',
  'scan.startup.mode' = 'latest-offset',   -- earliest 로 하면 과거 메시지까지 읽어 창이 쏟아진다
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- 티켓별 1분 텀블링 윈도우 예매 건수.
-- 이건 한 번 돌고 끝나는 쿼리가 아니라 계속 떠 있는 잡이다(웹 UI http://localhost:8081).
-- ⚠️ 윈도우는 벽시계가 아니라 '새 데이터'가 밀어야 닫힌다. 트래픽이 끊기면 마지막 창은 안 닫힌다.
SELECT window_start, window_end, ticketId, COUNT(*) AS cnt
FROM TABLE(
  TUMBLE(TABLE reservations, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, ticketId;
