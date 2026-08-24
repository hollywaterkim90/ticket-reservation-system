package org.example.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.domain.OutboxEvent;
import org.example.domain.OutboxStatus;
import org.example.dto.TicketReservationDto;
import org.example.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 파트 B — 릴레이가 outbox 의 NEW 행을 <b>실제 Kafka 토픽으로 발행</b>하고 SENT 로 마킹하는지 검증.
 * 파트 A(=단위 성격, 실 Postgres)와 달리 여기선 Postgres + Kafka 컨테이너를 함께 띄우는 통합 테스트다.
 */
@SpringBootTest(
        classes = OutboxRelayTest.RelayTestApp.class,
        properties = {
                // 자동 폴링 사실상 끔(600s). 테스트에서 relay.publishPending() 를 직접 1회 호출해 타이밍을 통제.
                "outbox.relay.interval-ms=600000",
                // 운영 설정(update)을 물려받지 않고 테스트에서만 스키마를 만들고 지운다.
                "spring.jpa.hibernate.ddl-auto=create-drop"})
@Testcontainers
class OutboxRelayTest {

    /**
     * 릴레이 경로에 필요한 빈만 올리는 테스트 전용 부트 클래스.
     * <p>
     * 운영 진입점({@code Main})을 쓰면 {@code org.example.listener} 까지 스캔되어
     * TicketConsumerListener 가 Elasticsearch 리포지토리를 요구하는데, 릴레이 검증과는 무관하다.
     * 스캔 범위를 outbox 로 좁혀 리스너 자체가 생성되지 않게 한다.
     * (ES 자동설정을 제외할 필요도 없어진다 — 쓰는 빈이 없으므로)
     */
    @SpringBootApplication(scanBasePackages = "org.example.outbox")
    @EnableJpaRepositories(basePackages = "org.example.repository")
    @EntityScan(basePackages = "org.example.domain")
    static class RelayTestApp {
    }

    // @ServiceConnection: 컨테이너가 뜨면 DataSource / spring.kafka.bootstrap-servers 를 자동 주입.
    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    @Autowired OutboxRelay relay;
    @Autowired OutboxEventRepository outboxRepository;
    @Autowired ObjectMapper objectMapper;

    @BeforeEach
    void clean() {
        // 릴레이는 NEW 행을 무조건 집어간다. 앞 테스트가 남긴 행이 섞이면 검증이 흔들리므로 비우고 시작한다.
        outboxRepository.deleteAll();
    }

    /**
     * 릴레이가 NEW 행을 실제 토픽으로 발행하고 SENT 로 마킹한다.
     * 여기서 처음으로 메시지가 브로커에 도달한다 — 프로세서는 적재만 했고 발행은 하지 않았다.
     */
    @Test
    void publishesNewRowAndMarksItSent() throws Exception {
        // given: 발행 대기(NEW) outbox 1건 — topic=ticket-payments, payload=DTO JSON
        TicketReservationDto dto = TicketReservationDto.builder()
                .userId("user1")
                .orderId("order-1")
                .ticketId("ticket:stock:god")
                .status("SUCCESS")
                .build();
        String outboxId = UUID.randomUUID().toString();
        outboxRepository.save(OutboxEvent.builder()
                .id(outboxId)
                .topic("ticket-payments")
                .msgKey(dto.getUserId())
                .payload(objectMapper.writeValueAsString(dto))
                .status(OutboxStatus.NEW)
                .createdAt(Instant.now())
                .build());

        // when: 릴레이 1회 수동 실행 (내부에서 SKIP LOCKED 로 NEW 행 잠그고 → 발행 → SENT)
        relay.publishPending();

        // then-1: 실제 ticket-payments 토픽에 그 메시지가 도착했는가
        ConsumerRecord<String, String> received = pollOne("ticket-payments");
        assertThat(received.key()).isEqualTo("user1");          // msgKey = userId
        assertThat(received.value()).contains("order-1");       // payload 에 orderId 포함

        // then-2: 발행 성공했으니 그 outbox 행은 SENT (실패였다면 NEW 로 남아 다음 주기 재시도 = at-least-once)
        assertThat(outboxRepository.findById(outboxId).orElseThrow().getStatus()).isEqualTo(OutboxStatus.SENT);
    }

    /**
     * at-least-once — 발행에 실패한 행은 SENT 로 넘어가지 않고 NEW 로 남아 다음 주기에 다시 집힌다.
     * 실패 행을 SENT 로 마킹해버리면 그 순간 이벤트는 영구 유실이고, Outbox 를 도입한 이유가 사라진다.
     * 재시도로 생기는 중복 발행은 소비측 멱등성(orderId)이 흡수한다.
     */
    @Test
    void keepsFailedRowAsNewForRetry() {
        // given: 릴레이가 발행 도중 실패하는 행 1건 (payload 가 DTO 로 역직렬화되지 않는다)
        //        브로커 ack 실패든 직렬화 실패든, 릴레이 입장에서는 "발행을 확정하지 못한 행"으로 같다.
        String outboxId = UUID.randomUUID().toString();
        outboxRepository.save(OutboxEvent.builder()
                .id(outboxId)
                .topic("ticket-payments")
                .msgKey("user1")
                .payload("{ 이건 JSON 이 아니다 }")
                .status(OutboxStatus.NEW)
                .createdAt(Instant.now())
                .build());

        // when: 릴레이 1회 실행 — 개별 행의 실패가 배치 전체를 죽이면 안 된다
        relay.publishPending();

        // then
        assertThat(outboxRepository.findById(outboxId).orElseThrow().getStatus()).isEqualTo(OutboxStatus.NEW);

        // ⚠️ 남은 과제: 지금 구조는 재시도 횟수 제한이 없어 이런 독성 메시지가 영구히 재시도된다.
        //    (attempt_count + 임계치 초과 시 FAILED 격리가 정석)
    }

    // 검증용 컨슈머: earliest 로 처음부터 읽어, 구독이 발행보다 늦어도 메시지를 놓치지 않는다.
    // 리밸런싱으로 첫 poll 이 비어 올 수 있어 최대 15초 폴링 루프.
    private ConsumerRecord<String, String> pollOne(String topic) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "group.id", "test-verifier",
                "auto.offset.reset", "earliest",
                "key.deserializer", StringDeserializer.class.getName(),
                "value.deserializer", StringDeserializer.class.getName()))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) return records.iterator().next();
            }
            throw new AssertionError(topic + " 토픽에서 메시지를 받지 못함(15s 타임아웃)");
        }
    }
}
