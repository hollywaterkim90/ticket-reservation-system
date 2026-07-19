package org.example.elasticsearch.document;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "ticket-reservations") // 엘라스틱서치 인덱스 지정
@Getter
@Setter
@NoArgsConstructor
public class TicketReservationDocument {

    @Id
    private String id; // 엘라스틱서치는 String 자동 생성 ID를 주로 씁니다.

    private String userId;
    private String ticketId;

    public TicketReservationDocument(String userId, String ticketId) {
        this.userId = userId;
        this.ticketId = ticketId;
    }
}