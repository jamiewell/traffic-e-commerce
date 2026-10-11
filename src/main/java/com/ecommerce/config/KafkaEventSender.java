package com.ecommerce.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

// key = orderUUID, header = eventType / eventId
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventSender {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    // 트랜잭션 중이면 커밋 후 발행
    public void send(String topic, UUID orderUUID, String eventType, byte[] payload) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    doSend(topic, orderUUID, eventType, payload);
                }
            });
            return;
        }
        doSend(topic, orderUUID, eventType, payload);
    }

    private void doSend(String topic, UUID orderUUID, String eventType, byte[] payload) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, orderUUID.toString(), payload);
        record.headers().add(EventTypes.HEADER_EVENT_TYPE, eventType.getBytes(StandardCharsets.UTF_8));
        record.headers().add(EventTypes.HEADER_EVENT_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record).whenComplete((result, e) -> {
            if (e != null) {
                log.error("Fail : publish {} to {} : orderUUID={}, error={}", eventType, topic, orderUUID, e.getMessage());
            } else {
                log.info("Publish {} to {} : orderUUID={}", eventType, topic, orderUUID);
            }
        });
    }

}
