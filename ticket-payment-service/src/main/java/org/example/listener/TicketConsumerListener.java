package org.example.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.TicketReservationDto;
import org.example.service.PaymentProcessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 예매 이벤트를 소비해 결제를 처리한다. 결제 결과의 ES 색인은 {@code org.example.indexer} 가 담당한다.
 * (같은 파이프라인이지만 관심사와 장애 영향 범위가 달라 분리했다.)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TicketConsumerListener {

    private final PaymentProcessor paymentProcessor;

    // 결제 워커 스레드 풀 격리: Kafka 메인 리스너 스레드와 분리해 PG 연동(블로킹)을 병렬 처리한다.
    private final ExecutorService paymentExecutor = Executors.newFixedThreadPool(50);

    /**
     * 배치의 각 메시지를 {@link PaymentProcessor#processAndStage} 로 처리한다.
     * 각 호출은 독립 트랜잭션에서 "결제 결과 + outbox" 를 커밋한다(발행은 OutboxRelay 담당).
     * <p>
     * 모든 처리(=DB 커밋)가 끝난 뒤에만 ack 한다. ack 전에 크래시가 나면 배치가 재처리되지만,
     * PaymentProcessor 의 orderId 멱등성이 중복 결제/중복 이벤트를 막는다(at-least-once).
     */
    @KafkaListener(topics = "ticket-reservations", groupId = "${custom.kafka.groups.payment}",
            containerFactory = "manualAckKafkaListenerContainerFactory")
    public void consumeReservation(List<TicketReservationDto> records, Acknowledgment ack) {
        log.info("📦 [consumeReservation] 메시지 수신: {}건", records.size());

        List<CompletableFuture<Void>> futures = records.stream()
                .map(event -> CompletableFuture.runAsync(() -> paymentProcessor.processAndStage(event), paymentExecutor))
                .toList();

        // 모든 결제 처리(=DB 커밋) 완료까지 대기. 하나라도 예외면 join 이 throw → ack 미실행 → 재처리.
        futures.forEach(CompletableFuture::join);

        ack.acknowledge();
        log.info("✅ [Batch Finished] 오프셋 커밋 완료 ({}건)", records.size());
    }
}
