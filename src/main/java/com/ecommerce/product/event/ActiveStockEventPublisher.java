package com.ecommerce.product.event;


import com.ecommerce.config.EventTypes;
import com.ecommerce.config.KafkaEventSender;
import com.ecommerce.config.KafkaTopics;
import com.ecommerce.product.dto.response.StockDeductResult;
import com.ecommerce.product.dto.response.StockRestoreResult;
import com.ecommerce.proto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ActiveStockEventPublisher {

    private final KafkaEventSender eventSender;

    // 주문 여부 확인
    public void publishStockActive(UUID orderUUID, List<StockDeductResult> results) {
        StockCheckResponseEvent.Builder eventBuilder = StockCheckResponseEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setAvailable(results.stream().allMatch(StockDeductResult::isSuccess))
                .setCheckedDt(System.currentTimeMillis());

        for (StockDeductResult result : results) {
            if (result.hasFailed()) {
                eventBuilder.addLackedItems(toLackedItem(result));
            }
        }

        StockCheckResponseEvent event = eventBuilder.build();
        eventSender.send(KafkaTopics.PRODUCT_EVENTS, orderUUID, EventTypes.STOCK_CHECK_RESPONSE, event.toByteArray());
        log.info("Publish stock active: orderUUID={}, available={}", orderUUID, event.getAvailable());
    }


    // 재고 차감 성공
    public void publishStockDeduct(UUID orderUUID, UUID productUUID, int quantity, int remainingStock) {
        StockDeductEvent event = StockDeductEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .addItems(DeductItems.newBuilder()
                        .setProductUuid(productUUID.toString())
                        .setDeductQuantity(quantity)
                        .setRemainingStock(remainingStock)
                        .build())
                .setDeductDt(System.currentTimeMillis())
                .build();

        eventSender.send(KafkaTopics.PRODUCT_EVENTS, orderUUID, EventTypes.STOCK_DEDUCTED, event.toByteArray());
        log.info("Publish deduct item: orderUUID={}, productUUID={}", orderUUID, productUUID);
    }

    // 재고 부족 이벤트
    public void publishStockLacked(UUID orderUUID, UUID productUUID, int requestedQuantity, int availableStock) {
        StockDeductLackedEvent event = StockDeductLackedEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .addItems(LackedItems.newBuilder()
                        .setProductUuid(productUUID.toString())
                        .setRequestedQuantity(requestedQuantity)
                        .setAvailableStock(availableStock)
                        .build())
                .setCheckedDt(System.currentTimeMillis())
                .build();

        eventSender.send(KafkaTopics.PRODUCT_EVENTS, orderUUID, EventTypes.STOCK_LACKED, event.toByteArray());
        log.info("Publish stock lacked: orderUUID={}, productUUID={}", orderUUID, productUUID);
    }

    // 재고 복구
    public void publishStockRestored(UUID orderUUID, List<StockRestoreResult> results) {
        StockRestoredEvent.Builder eventBuilder = StockRestoredEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setRestoredDt(System.currentTimeMillis());

        for (StockRestoreResult result : results) {
            eventBuilder.addItems(RestoredItems.newBuilder()
                    .setProductUuid(result.getProductUUID().toString())
                    .setRestoredQuantity(result.getRestoredQuantity())
                    .setCurrentStock(result.getCurrentStock())
                    .build());
        }

        eventSender.send(KafkaTopics.PRODUCT_EVENTS, orderUUID, EventTypes.STOCK_RESTORED, eventBuilder.build().toByteArray());
        log.info("Publish stock restored: orderUUID={}, itemCount={}", orderUUID, results.size());
    }


    // 배치 재고 차감
    public void publishBulkStockDeduct(UUID orderUUID, List<StockDeductResult> results) {
        StockDeductEvent.Builder eventBuilder = StockDeductEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setDeductDt(System.currentTimeMillis());

        for (StockDeductResult result : results) {
            eventBuilder.addItems(DeductItems.newBuilder()
                    .setProductUuid(result.getProductUUID().toString())
                    .setDeductQuantity(result.getRequestedQuantity())
                    .setRemainingStock(result.getRemainingStock())
                    .build());
        }

        eventSender.send(KafkaTopics.PRODUCT_EVENTS, orderUUID, EventTypes.STOCK_BULK_DEDUCTED, eventBuilder.build().toByteArray());
        log.info("배치 재고 차감 이벤트 발행: 주문={}", orderUUID);
    }

    // 배치 재고 차감 실패
    public void publishBulkStockFailed(UUID orderUUID, List<StockDeductResult> results) {
        StockDeductLackedEvent.Builder eventBuilder = StockDeductLackedEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setCheckedDt(System.currentTimeMillis());

        for (StockDeductResult result : results) {
            if (result.hasFailed()) {
                eventBuilder.addItems(toLackedItem(result));
            }
        }

        eventSender.send(KafkaTopics.PRODUCT_EVENTS, orderUUID, EventTypes.STOCK_BULK_LACKED, eventBuilder.build().toByteArray());
        log.info("배치 재고 실패 이벤트 발행: 주문={}", orderUUID);
    }

    private LackedItems toLackedItem(StockDeductResult result) {
        return LackedItems.newBuilder()
                .setProductUuid(result.getProductUUID().toString())
                .setRequestedQuantity(result.getRequestedQuantity())
                .setAvailableStock(result.getRemainingStock())
                .build();
    }

}
