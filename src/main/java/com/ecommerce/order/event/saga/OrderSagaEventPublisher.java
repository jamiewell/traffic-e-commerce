package com.ecommerce.order.event.saga;

import com.ecommerce.config.EventTypes;
import com.ecommerce.config.KafkaEventSender;
import com.ecommerce.config.KafkaTopics;
import com.ecommerce.order.orderEntity.OrderItem;
import com.ecommerce.proto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaEventPublisher {

    private final KafkaEventSender eventSender;

    // 재고 확인 요청
    public void publishStockCheckRequested(UUID orderUUID, List<OrderItem> orderItems) {
        SagaStockCheckEvent.Builder eventBuilder = SagaStockCheckEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setDeductDt(System.currentTimeMillis());

        for (OrderItem item : orderItems) {
            eventBuilder.addItems(toDeductItem(item));
        }

        eventSender.send(KafkaTopics.PRODUCT_COMMANDS, orderUUID, EventTypes.STOCK_CHECK, eventBuilder.build().toByteArray());
        log.info("[Orchestrator] Publish stock check : orderUUID={}, itemCount={}", orderUUID, orderItems.size());
    }

    // 재고 차감 요청 : 상품 1개는 단순 차감, 2개 이상은 배치 차감
    public void publishStockDeductRequested(UUID orderUUID, List<OrderItem> orderItems) {
        SagaStockDeductEvent.Builder eventBuilder = SagaStockDeductEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setDeductDt(System.currentTimeMillis());

        for (OrderItem item : orderItems) {
            eventBuilder.addItems(toDeductItem(item));
        }

        String eventType = orderItems.size() == 1 ? EventTypes.STOCK_DEDUCT : EventTypes.STOCK_BULK_DEDUCT;
        eventSender.send(KafkaTopics.PRODUCT_COMMANDS, orderUUID, eventType, eventBuilder.build().toByteArray());
        log.info("[Orchestrator] Publish {} : orderUUID={}, itemCount={}", eventType, orderUUID, orderItems.size());
    }

    // 재고 복구 요청
    public void publishStockRestoreRequested(UUID orderUUID, List<OrderItem> orderItems) {
        SagaStockRestoreEvent.Builder eventBuilder = SagaStockRestoreEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setRestoredDt(System.currentTimeMillis());

        for (OrderItem item : orderItems) {
            eventBuilder.addItems(RestoreItem.newBuilder()
                    .setProductUuid(item.getProductUUID().toString())
                    .setRestoredQuantity(item.getQuantity())
                    .build());
        }

        eventSender.send(KafkaTopics.PRODUCT_COMMANDS, orderUUID, EventTypes.STOCK_RESTORE, eventBuilder.build().toByteArray());
        log.info("[Orchestrator] Publish stock restoration : orderUUID={}, itemCount={}", orderUUID, orderItems.size());
    }

    // 결제 요청
    public void publishPaymentRequested(UUID orderUUID, BigDecimal totalPrice) {
        SagaPaymentRequestEvent paymentRequest = SagaPaymentRequestEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setTotalPrice(totalPrice.doubleValue())
                .setRequestedDt(System.currentTimeMillis())
                .build();

        eventSender.send(KafkaTopics.PAYMENT_COMMANDS, orderUUID, EventTypes.PAYMENT_REQUEST, paymentRequest.toByteArray());
        log.info("[Orchestrator] Publish payment request : orderUUID={}, amount={}", orderUUID, totalPrice);
    }

    // 결제 취소 요청 (보상)
    public void publishPaymentCancellationRequested(UUID orderUUID) {
        SagaPaymentCancelRequestEvent cancelRequest = SagaPaymentCancelRequestEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setRequestedDt(System.currentTimeMillis())
                .build();

        eventSender.send(KafkaTopics.PAYMENT_COMMANDS, orderUUID, EventTypes.PAYMENT_CANCEL, cancelRequest.toByteArray());
        log.info("[Orchestrator] Publish payment cancellation : orderUUID={}", orderUUID);
    }

    // 배송 요청
    public void publishShipmentRequested(UUID orderUUID, UUID userId, String address, String contact, List<OrderItem> orderItems) {
        SagaShipmentRequestEvent.Builder shipmentRequest = SagaShipmentRequestEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setUserId(userId.toString())
                .setDeliveryAddress(address)
                .setContactPhone(contact)
                .setRequestedDt(System.currentTimeMillis());

        for (OrderItem item : orderItems) {
            shipmentRequest.addItems(ShipmentItem.newBuilder()
                    .setProductUuid(item.getProductUUID().toString())
                    .setQuantity(item.getQuantity())
                    .setPrice(item.getPrice().doubleValue())
                    .build());
        }

        eventSender.send(KafkaTopics.SHIPMENT_COMMANDS, orderUUID, EventTypes.SHIPMENT_REQUEST, shipmentRequest.build().toByteArray());
        log.info("[Orchestrator] Publish shipment request : orderUUID={}", orderUUID);
    }

    private DeductItem toDeductItem(OrderItem item) {
        return DeductItem.newBuilder()
                .setProductUuid(item.getProductUUID().toString())
                .setDeductQuantity(item.getQuantity())
                .build();
    }

}
