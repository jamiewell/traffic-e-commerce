package com.ecommerce.order.event;

import com.ecommerce.config.EventTypes;
import com.ecommerce.config.KafkaEventSender;
import com.ecommerce.config.KafkaTopics;
import com.ecommerce.order.orderEntity.Order;
import com.ecommerce.order.orderEntity.OrderItem;
import com.ecommerce.proto.OrderCancelledEvent;
import com.ecommerce.proto.OrderCreatedEvent;
import com.ecommerce.proto.OrderCreatedItem;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaEventSender eventSender;

    // 주문 생성
    public void publishOrderCreated(Order order) {
        var protoBuilder = OrderCreatedEvent.newBuilder()
                .setOrderUUID(order.getOrderUUID().toString())
                .setUserId(order.getUserId().toString())
                .setAddress(order.getAddress())
                .setContact(order.getContact())
                .setTotalPrice(order.getTotalPrice().doubleValue())
                .setStatus(order.getStatus().name());

        for (OrderItem item : order.getOrderItems()) {
            protoBuilder.addItems(
                    OrderCreatedItem.newBuilder()
                            .setProductUUID(item.getProductUUID().toString())
                            .setQuantity(item.getQuantity())
                            .setPrice(item.getPrice().doubleValue())
                            .build()
            );
        }

        eventSender.send(KafkaTopics.ORDER_EVENTS, order.getOrderUUID(), EventTypes.ORDER_CREATED, protoBuilder.build().toByteArray());
    }

    // 주문 취소
    public void publishOrderCancelled(UUID orderUUID) {
        OrderCancelledEvent cancelledEvent = OrderCancelledEvent.newBuilder()
                .setOrderUuid(orderUUID.toString())
                .setCancelledDt(System.currentTimeMillis())
                .build();

        eventSender.send(KafkaTopics.ORDER_EVENTS, orderUUID, EventTypes.ORDER_CANCELLED, cancelledEvent.toByteArray());
    }

}
