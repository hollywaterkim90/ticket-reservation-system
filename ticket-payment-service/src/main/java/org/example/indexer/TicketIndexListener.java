package org.example.indexer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.TicketReservationDto;
import org.example.indexer.document.TicketReservationDocument;
import org.example.indexer.repository.TicketReservationElasticRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * 확정된 결제 결과를 Elasticsearch 에 색인한다.
 * <p>
 * 결제 처리({@code org.example.listener})와 <b>같은 토픽을 다른 컨슈머 그룹으로</b> 소비하므로 서로 독립적이다.
 * ES 가 죽어도 색인만 밀릴 뿐 결제는 계속되고, 반대도 마찬가지다.
 * <p>
 * 이 패키지는 색인에 필요한 것(리스너·문서·리포지토리)을 모두 소유한다.
 * 나중에 별도 서비스로 떼어낼 때 패키지째 들어내면 되도록 경계를 미리 그어둔 것이다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TicketIndexListener {

    private final TicketReservationElasticRepository repository;

    @KafkaListener(topics = "ticket-payments", groupId = "${custom.kafka.groups.indexer}")
    public void consumePayment(List<TicketReservationDto> records) {
        List<TicketReservationDocument> documents = records.stream()
                .filter(Objects::nonNull)
                .map(event -> TicketReservationDocument.builder()
                        .orderId(event.getOrderId())
                        .status(event.getStatus())
                        .timestamp(Instant.now())
                        .build())
                .toList();

        if (!documents.isEmpty()) {
            repository.saveAll(documents);
            log.info("🔎 [색인] {}건 저장", documents.size());
        }
    }
}
