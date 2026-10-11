package com.ecommerce.order.orderService;

import com.ecommerce.order.dto.orderRequest.OrderCreateRequest;
import com.ecommerce.order.dto.orderResponse.OrderItemResponse;
import com.ecommerce.order.dto.orderResponse.OrderResponse;
import com.ecommerce.order.event.OrderEventPublisher;
import com.ecommerce.order.orderEntity.Order;
import com.ecommerce.order.orderRepository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher orderEventPublisher;


    @Transactional
    public OrderResponse createOrder(UUID userId, OrderCreateRequest request) {


        BigDecimal totalPrice = request.getItems().stream().map(i -> i.getPrice().multiply(BigDecimal.valueOf(i.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);


        Order createdOrder = Order.of(userId, request, totalPrice, request.getItems());
        Order savedOrder = orderRepository.save(createdOrder);

        orderEventPublisher.publishOrderCreated(savedOrder);


        return buildResponse(savedOrder);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrderDetail(UUID orderUUID) {
        Order order = orderRepository.findByOrderUUID(orderUUID).orElseThrow(() -> new IllegalArgumentException("Cannot find order"));
        return buildResponse(order);
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getOrderListByUser(UUID userId) {
        return orderRepository.findAllByUserId(userId).stream().map(this::buildResponse).toList();
    }

    @Transactional
    public OrderResponse cancelOrder(UUID orderUUID) {
        Order order = orderRepository.findByOrderUUID(orderUUID).orElseThrow(() -> new IllegalArgumentException("Cannot find order"));
        if (!order.getStatus().isCancellableByUser()) {
            throw new IllegalStateException("Cannot cancel order : status=" + order.getStatus());
        }

        // 보상은 오케스트레이터가 order.cancelled 수신 후 처리
        order.cancel();
        orderEventPublisher.publishOrderCancelled(orderUUID);

        return buildResponse(order);
    }


    private OrderResponse buildResponse(Order order) {
        return OrderResponse.builder()
                .orderUUID(order.getOrderUUID())
                .status(order.getStatus())
                .totalPrice(order.getTotalPrice())
                .address(order.getAddress())
                .contact(order.getContact())
                .items(order.getOrderItems().stream().map(i ->
                        OrderItemResponse.builder()
                                .productUUID(i.getProductUUID())
                                .quantity(i.getQuantity())
                                .price(i.getPrice())
                                .build()
                ).toList())
                .build();
    }



}
