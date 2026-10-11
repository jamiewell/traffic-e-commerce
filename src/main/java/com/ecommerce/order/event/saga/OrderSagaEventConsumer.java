package com.ecommerce.order.event.saga;

import com.ecommerce.config.EventTypes;
import com.ecommerce.config.KafkaTopics;
import com.ecommerce.order.orderService.OrderSagaOrchestrator;
import com.ecommerce.proto.*;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

// 예외는 던져서 컨테이너 에러 핸들러가 재시도하도록 함
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaEventConsumer {

    private final OrderSagaOrchestrator orchestrator;

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = KafkaTopics.ORDER_SERVICE_GROUP)
    public void consumeOrderEvents(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String eventType = EventTypes.resolveEventType(record);
        byte[] value = record.value();
        log.info("[Orchestrator] Consume order event : {}", eventType);

        switch (eventType) {
            case EventTypes.ORDER_CREATED -> operateOrderEvent(value);
            case EventTypes.ORDER_CANCELLED -> cancelOrderEvent(value);
            default -> log.debug("[Orchestrator] skip order event : {}", eventType);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = KafkaTopics.PRODUCT_EVENTS, groupId = KafkaTopics.ORDER_SERVICE_GROUP)
    public void consumeProductEvents(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String eventType = EventTypes.resolveEventType(record);
        byte[] value = record.value();
        log.info("[Orchestrator] Consume product event : {}", eventType);

        switch (eventType) {
            case EventTypes.STOCK_CHECK_RESPONSE -> operateStockCheckResponseEvent(value);
            case EventTypes.STOCK_DEDUCTED -> operateProductDeductedEvent(value);
            case EventTypes.STOCK_BULK_DEDUCTED -> operateStockBulkDeductedEvent(value);
            case EventTypes.STOCK_LACKED, EventTypes.STOCK_BULK_LACKED -> operateProductLackedEvent(value);
            case EventTypes.STOCK_RESTORED -> operateStockRestoredEvent(value);
            default -> log.debug("[Orchestrator] skip product event : {}", eventType);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = KafkaTopics.ORDER_SERVICE_GROUP)
    public void consumePaymentEvents(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String eventType = EventTypes.resolveEventType(record);
        byte[] value = record.value();
        log.info("[Orchestrator] Consume payment event : {}", eventType);

        switch (eventType) {
            case EventTypes.PAYMENT_SUCCEEDED -> operatePaymentSuccess(value);
            case EventTypes.PAYMENT_FAILED -> operatePaymentFailed(value);
            case EventTypes.PAYMENT_CANCELLED -> operatePaymentCancelled(value);
            default -> log.debug("[Orchestrator] skip payment event : {}", eventType);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = KafkaTopics.SHIPMENT_EVENTS, groupId = KafkaTopics.ORDER_SERVICE_GROUP)
    public void consumeShipmentEvents(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String eventType = EventTypes.resolveEventType(record);
        byte[] value = record.value();
        log.info("[Orchestrator] Consume shipment event : {}", eventType);

        switch (eventType) {
            case EventTypes.SHIPMENT_CREATED -> operateShipmentCreated(value);
            case EventTypes.SHIPMENT_FAILED -> operateShipmentFailed(value);
            case EventTypes.SHIPMENT_COMPLETED -> operateShipmentCompleted(value);
            default -> log.debug("[Orchestrator] skip shipment event : {}", eventType);
        }
        ack.acknowledge();
    }


    private void operateOrderEvent(byte[] value) {
        orchestrator.startOrderSaga(parseOrderCreated(value));
    }

    private void cancelOrderEvent(byte[] value) {
        orchestrator.cancelOrderSaga(parseOrderCancelled(value));
    }

    private void operateStockCheckResponseEvent(byte[] value) {
        orchestrator.handleStockCheckResponse(parse(() -> StockCheckResponseEvent.parseFrom(value), "stock check response"));
    }

    private void operateProductDeductedEvent(byte[] value) {
        orchestrator.handleStockDeductSuccess(parse(() -> StockDeductEvent.parseFrom(value), "stock deducted"));
    }

    private void operateStockBulkDeductedEvent(byte[] value) {
        orchestrator.handleStockBulkDeducted(parse(() -> StockDeductEvent.parseFrom(value), "stock bulk deducted"));
    }

    private void operateProductLackedEvent(byte[] value) {
        orchestrator.handleStockLacked(parse(() -> StockDeductLackedEvent.parseFrom(value), "stock lacked"));
    }

    private void operateStockRestoredEvent(byte[] value) {
        orchestrator.handleStockRestored(parse(() -> StockRestoredEvent.parseFrom(value), "stock restored"));
    }

    private void operatePaymentSuccess(byte[] value) {
        orchestrator.handlePaymentSuccess(parse(() -> PaymentSuccessEvent.parseFrom(value), "payment success"));
    }

    private void operatePaymentFailed(byte[] value) {
        orchestrator.handlePaymentFailed(parse(() -> PaymentFailedEvent.parseFrom(value), "payment failed"));
    }

    private void operatePaymentCancelled(byte[] value) {
        orchestrator.handlePaymentCancelled(parse(() -> PaymentCancelledEvent.parseFrom(value), "payment cancelled"));
    }

    private void operateShipmentCreated(byte[] value) {
        orchestrator.handleShipmentCreated(parse(() -> ShipmentCreatedEvent.parseFrom(value), "shipment created"));
    }

    private void operateShipmentFailed(byte[] value) {
        orchestrator.handleShipmentFailed(parse(() -> ShipmentFailedEvent.parseFrom(value), "shipment failed"));
    }

    private void operateShipmentCompleted(byte[] value) {
        orchestrator.handleShipmentCompleted(parse(() -> ShipmentCompletedEvent.parseFrom(value), "shipment completed"));
    }

    private UUID parseOrderCreated(byte[] eventBytes) {
        OrderCreatedEvent orderCreated = parse(() -> OrderCreatedEvent.parseFrom(eventBytes), "order created");
        return UUID.fromString(orderCreated.getOrderUUID());
    }

    private UUID parseOrderCancelled(byte[] eventBytes) {
        OrderCancelledEvent orderCancelled = parse(() -> OrderCancelledEvent.parseFrom(eventBytes), "order cancelled");
        return UUID.fromString(orderCancelled.getOrderUuid());
    }

    private <T> T parse(ProtoParser<T> parser, String eventName) {
        try {
            return parser.parse();
        } catch (InvalidProtocolBufferException e) {
            log.error("[Orchestrator] parsing {} error : {}", eventName, e.getMessage());
            throw new IllegalArgumentException("Failed to parse " + eventName, e);
        }
    }

    @FunctionalInterface
    private interface ProtoParser<T> {
        T parse() throws InvalidProtocolBufferException;
    }

}
