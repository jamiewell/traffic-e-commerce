package com.ecommerce.product.event;

import com.ecommerce.config.EventTypes;
import com.ecommerce.config.KafkaTopics;
import com.ecommerce.product.dto.request.StockDeductRequest;
import com.ecommerce.product.productService.ProductService;
import com.ecommerce.proto.DeductItem;
import com.ecommerce.proto.SagaStockCheckEvent;
import com.ecommerce.proto.SagaStockDeductEvent;
import com.ecommerce.proto.SagaStockRestoreEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

// 예외는 던져서 컨테이너 에러 핸들러가 재시도하도록 함
@Slf4j
@Service
@RequiredArgsConstructor
public class ActiveStockEventConsumer {

    private final ProductService productService;

    @KafkaListener(topics = KafkaTopics.PRODUCT_COMMANDS, groupId = KafkaTopics.PRODUCT_SERVICE_GROUP)
    public void consumeOrderEvents(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String eventType = EventTypes.resolveEventType(record);
        byte[] value = record.value();
        log.info("Stock consume: eventType={}, key={}", eventType, record.key());

        switch (eventType) {
            case EventTypes.STOCK_CHECK -> operateStockCheck(value);
            case EventTypes.STOCK_DEDUCT -> operateStockDeduction(value);
            case EventTypes.STOCK_BULK_DEDUCT -> operateBulkStockDeduction(value);
            case EventTypes.STOCK_RESTORE -> operateStockRestoration(value);
            default -> log.debug("Stock skip event : {}", eventType);
        }
        ack.acknowledge();
    }

    private void operateStockCheck(byte[] value) {
        SagaStockCheckEvent checkEvent = parse(() -> SagaStockCheckEvent.parseFrom(value), "stock check");
        productService.hasStockActive(toRequestList(checkEvent.getItemsList()), UUID.fromString(checkEvent.getOrderUuid()));
    }

    private void operateStockDeduction(byte[] value) {
        SagaStockDeductEvent deductEvent = parse(() -> SagaStockDeductEvent.parseFrom(value), "stock deduct");
        DeductItem item = deductEvent.getItems(0);
        productService.deductStock(UUID.fromString(item.getProductUuid()), item.getDeductQuantity(), UUID.fromString(deductEvent.getOrderUuid()));
    }

    private void operateBulkStockDeduction(byte[] value) {
        SagaStockDeductEvent deductEvent = parse(() -> SagaStockDeductEvent.parseFrom(value), "stock bulk deduct");
        productService.deductStocks(toRequestList(deductEvent.getItemsList()), UUID.fromString(deductEvent.getOrderUuid()));
    }

    // 복구 수량은 Product 의 차감 이력 기준
    private void operateStockRestoration(byte[] value) {
        SagaStockRestoreEvent restoreEvent = parse(() -> SagaStockRestoreEvent.parseFrom(value), "stock restore");
        productService.restoreStocks(UUID.fromString(restoreEvent.getOrderUuid()));
    }

    private List<StockDeductRequest> toRequestList(List<DeductItem> items) {
        return items.stream()
                .map(item -> new StockDeductRequest(UUID.fromString(item.getProductUuid()), item.getDeductQuantity()))
                .toList();
    }

    private <T> T parse(ProtoParser<T> parser, String eventName) {
        try {
            return parser.parse();
        } catch (InvalidProtocolBufferException e) {
            log.error("Parsing {} error: {}", eventName, e.getMessage());
            throw new IllegalArgumentException("Failed to parse " + eventName, e);
        }
    }

    @FunctionalInterface
    private interface ProtoParser<T> {
        T parse() throws InvalidProtocolBufferException;
    }

}
