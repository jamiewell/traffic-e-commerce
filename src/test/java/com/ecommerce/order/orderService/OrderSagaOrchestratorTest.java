package com.ecommerce.order.orderService;

import com.ecommerce.order.event.saga.OrderSagaEventPublisher;
import com.ecommerce.order.orderEntity.Order;
import com.ecommerce.order.orderEntity.OrderItem;
import com.ecommerce.order.orderEntity.OrderSaga;
import com.ecommerce.order.orderRepository.OrderRepository;
import com.ecommerce.order.orderRepository.OrderSagaRepository;
import com.ecommerce.order.status.*;
import com.ecommerce.proto.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderSagaOrchestratorTest {

    @Mock
    private OrderRepository orderRepository;
    @Mock
    private OrderSagaRepository orderSagaRepository;
    @Mock
    private OrderSagaEventPublisher sagaEventPublisher;
    @InjectMocks
    private OrderSagaOrchestrator orchestrator;

    private final UUID orderUUID = UUID.randomUUID();
    private Order order;

    @BeforeEach
    void setUp() {
        order = Order.builder()
                .orderUUID(orderUUID)
                .userId(UUID.randomUUID())
                .status(OrderStatus.ORDERED)
                .address("seoul")
                .contact("010-0000-0000")
                .totalPrice(new BigDecimal("1000"))
                .build();
        order.getOrderItems().add(OrderItem.builder().order(order).productUUID(UUID.randomUUID()).quantity(1).price(new BigDecimal("1000")).build());
        lenient().when(orderRepository.findByOrderUUID(orderUUID)).thenReturn(Optional.of(order));
    }

    private OrderSaga givenSaga(SagaStatus sagaStatus, SagaStep step) {
        OrderSaga saga = OrderSaga.builder()
                .orderUUID(orderUUID)
                .sagaStatus(sagaStatus)
                .currentStep(step)
                .productStatus(ProductStatus.PENDING)
                .paymentStatus(PaymentStatus.PENDING)
                .shipmentStatus(ShipmentStatus.PENDING)
                .build();
        given(orderSagaRepository.findByOrderUUID(orderUUID)).willReturn(Optional.of(saga));
        return saga;
    }

    private void moveOrderTo(OrderStatus... statuses) {
        for (OrderStatus status : statuses) {
            order.updateStatus(status);
        }
    }

    @Test
    @DisplayName("order.created -> 사가 생성, 재고 확인 요청")
    void startOrderSaga() {
        given(orderSagaRepository.findByOrderUUID(orderUUID)).willReturn(Optional.empty());

        orchestrator.startOrderSaga(orderUUID);

        verify(orderSagaRepository).save(any(OrderSaga.class));
        verify(sagaEventPublisher).publishStockCheckRequested(eq(orderUUID), anyList());
        then(order.getStatus()).isEqualTo(OrderStatus.INVENTORY_REQUESTED);
    }

    @Test
    @DisplayName("order.created 중복 수신 -> 무시")
    void startOrderSaga_duplicate() {
        givenSaga(SagaStatus.IN_PROGRESS, SagaStep.STOCK);

        orchestrator.startOrderSaga(orderUUID);

        verify(orderSagaRepository, never()).save(any());
        verifyNoInteractions(sagaEventPublisher);
    }

    @Test
    @DisplayName("재고 확인 성공 -> 재고 차감 요청")
    void stockCheckAvailable() {
        givenSaga(SagaStatus.IN_PROGRESS, SagaStep.STOCK);
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED);

        orchestrator.handleStockCheckResponse(StockCheckResponseEvent.newBuilder().setOrderUuid(orderUUID.toString()).setAvailable(true).build());

        verify(sagaEventPublisher).publishStockDeductRequested(eq(orderUUID), anyList());
    }

    @Test
    @DisplayName("재고 확인 부족 -> 주문 취소, 사가 FAILED, 보상 없음")
    void stockCheckLacked() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.STOCK);
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED);

        orchestrator.handleStockCheckResponse(StockCheckResponseEvent.newBuilder().setOrderUuid(orderUUID.toString()).setAvailable(false).build());

        then(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        then(saga.getSagaStatus()).isEqualTo(SagaStatus.FAILED);
        verifyNoInteractions(sagaEventPublisher);
    }

    @Test
    @DisplayName("재고 차감 성공 -> 결제 요청")
    void stockDeducted() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.STOCK);
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED);

        orchestrator.handleStockBulkDeducted(StockDeductEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());

        then(saga.getProductStatus()).isEqualTo(ProductStatus.DEDUCTED);
        then(saga.getCurrentStep()).isEqualTo(SagaStep.PAYMENT);
        then(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_REQUESTED);
        verify(sagaEventPublisher).publishPaymentRequested(orderUUID, order.getTotalPrice());
    }

    @Test
    @DisplayName("재고 차감 결과 중복 수신 -> 무시")
    void stockDeducted_duplicate() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.STOCK);
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED);
        StockDeductEvent event = StockDeductEvent.newBuilder().setOrderUuid(orderUUID.toString()).build();

        orchestrator.handleStockDeductSuccess(event);
        orchestrator.handleStockDeductSuccess(event);

        verify(sagaEventPublisher, times(1)).publishPaymentRequested(any(), any());
    }

    @Test
    @DisplayName("결제 성공 -> 배송 요청")
    void paymentSuccess() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.PAYMENT);
        saga.completeStockDeduction();
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED, OrderStatus.INVENTORY_CONFIRMED, OrderStatus.PAYMENT_REQUESTED);

        orchestrator.handlePaymentSuccess(PaymentSuccessEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());

        then(order.getStatus()).isEqualTo(OrderStatus.PAID);
        then(saga.getCurrentStep()).isEqualTo(SagaStep.SHIPMENT);
        verify(sagaEventPublisher).publishShipmentRequested(eq(orderUUID), any(), any(), any(), anyList());
    }

    @Test
    @DisplayName("결제 실패 -> 재고 복구 요청, 복구 완료 시 COMPENSATED")
    void paymentFailed() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.PAYMENT);
        saga.completeStockDeduction();
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED, OrderStatus.INVENTORY_CONFIRMED, OrderStatus.PAYMENT_REQUESTED);

        orchestrator.handlePaymentFailed(PaymentFailedEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());

        then(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        then(saga.getSagaStatus()).isEqualTo(SagaStatus.COMPENSATING);
        verify(sagaEventPublisher).publishStockRestoreRequested(eq(orderUUID), anyList());
        verify(sagaEventPublisher, never()).publishPaymentCancellationRequested(any());

        orchestrator.handleStockRestored(StockRestoredEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());

        then(saga.getProductStatus()).isEqualTo(ProductStatus.RESTORED);
        then(saga.getSagaStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    @DisplayName("배송 생성 -> SHIPPED, 사가 COMPLETED / 배송 완료 -> DELIVERED")
    void shipmentCreatedAndCompleted() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.SHIPMENT);
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED, OrderStatus.INVENTORY_CONFIRMED, OrderStatus.PAYMENT_REQUESTED, OrderStatus.PAID);

        orchestrator.handleShipmentCreated(ShipmentCreatedEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());
        then(order.getStatus()).isEqualTo(OrderStatus.SHIPPED);
        then(saga.getSagaStatus()).isEqualTo(SagaStatus.COMPLETED);

        orchestrator.handleShipmentCompleted(ShipmentCompletedEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());
        then(order.getStatus()).isEqualTo(OrderStatus.DELIVERED);
    }

    @Test
    @DisplayName("배송 생성 실패 -> 결제 취소 + 재고 복구")
    void shipmentFailed() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.SHIPMENT);
        saga.completeStockDeduction();
        saga.completePayment();
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED, OrderStatus.INVENTORY_CONFIRMED, OrderStatus.PAYMENT_REQUESTED, OrderStatus.PAID);

        orchestrator.handleShipmentFailed(ShipmentFailedEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());

        then(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(sagaEventPublisher).publishPaymentCancellationRequested(orderUUID);
        verify(sagaEventPublisher).publishStockRestoreRequested(eq(orderUUID), anyList());
    }

    @Test
    @DisplayName("결제 대기 중 사용자 취소 -> 재고 복구, 늦게 온 결제 성공 -> 결제 취소만 (재고 복구 중복 없음)")
    void cancelDuringPayment_latePaymentSuccess() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.PAYMENT);
        saga.completeStockDeduction();
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED, OrderStatus.INVENTORY_CONFIRMED, OrderStatus.PAYMENT_REQUESTED, OrderStatus.CANCELLED);

        orchestrator.cancelOrderSaga(orderUUID);
        orchestrator.handlePaymentSuccess(PaymentSuccessEvent.newBuilder().setOrderUuid(orderUUID.toString()).build());

        verify(sagaEventPublisher, times(1)).publishStockRestoreRequested(eq(orderUUID), anyList());
        verify(sagaEventPublisher, times(1)).publishPaymentCancellationRequested(orderUUID);
        verify(sagaEventPublisher, never()).publishShipmentRequested(any(), any(), any(), any(), anyList());
        then(saga.getSagaStatus()).isEqualTo(SagaStatus.COMPENSATING);
    }

    @Test
    @DisplayName("재고 확인 중 사용자 취소 -> 보상할 것 없음, 바로 COMPENSATED")
    void cancelDuringStockCheck() {
        OrderSaga saga = givenSaga(SagaStatus.IN_PROGRESS, SagaStep.STOCK);
        moveOrderTo(OrderStatus.INVENTORY_REQUESTED, OrderStatus.CANCELLED);

        orchestrator.cancelOrderSaga(orderUUID);

        then(saga.getSagaStatus()).isEqualTo(SagaStatus.COMPENSATED);
        verifyNoInteractions(sagaEventPublisher);
    }

    @Test
    @DisplayName("허용되지 않은 주문 상태 전이 -> 예외")
    void invalidOrderTransition() {
        assertThatThrownBy(() -> order.updateStatus(OrderStatus.PAID))
                .isInstanceOf(IllegalStateException.class);
    }

}
