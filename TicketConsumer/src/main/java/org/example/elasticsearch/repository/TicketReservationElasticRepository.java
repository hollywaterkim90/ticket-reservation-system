package org.example.elasticsearch.repository;

import org.example.elasticsearch.document.TicketReservationDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface TicketReservationElasticRepository extends ElasticsearchRepository<TicketReservationDocument, String> {
}