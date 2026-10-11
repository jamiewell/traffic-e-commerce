package com.ecommerce.order.orderService;

import com.ecommerce.order.event.saga.OrderSagaEventPublisher;
import com.ecommerce.order.orderEntity.Order;
import com.ecommerce.order.orderEntity.OrderSaga;
import com.ecommerce.order.orderRepository.OrderRepository;
import com.ecommerce.order.orderRepository.OrderSagaRepository;
import com.ecommerce.order.status.*;
import com.ecommerce.proto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// 재고 -> 결제 -> 배송 순서 (src/designEDA.puml)
@Slf4j
@Service
@Transactional
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final OrderRepository orderRepository;
    private final OrderSagaRepository orderSagaRepository;
    private final OrderSagaEventPublisher sagaEventPublisher;

    // consume 주문 생성
    public void startOrderSaga(UUID orderUUID) {
        if (orderSagaRepository.findByOrderUUID(orderUUID).isPresent()) {
            log.info("[Orchestrator] saga already started : orderUUID={}", orderUUID);
            return;
        }

        Order order = findOrderByUUID(orderUUID);
        if (order.getStatus() != OrderStatus.ORDERED) {
            log.info("[Orchestrator] skip saga, order status={} : orderUUID={}", order.getStatus(), orderUUID);
            return;
        }

        log.info("[Orchestrator] start order : orderUUID={}", orderUUID);
        OrderSaga saga = OrderSaga.builder()
                .orderUUID(orderUUID)
                .sagaStatus(SagaStatus.STARTED)
                .currentStep(SagaStep.STOCK)
                .paymentStatus(PaymentStatus.PENDING)
                .productStatus(ProductStatus.PENDING)
                .shipmentStatus(ShipmentStatus.PENDING)
                .build();
        orderSagaRepository.save(saga);

        requestStockCheck(order, saga);
    }

    // consume 주문 취소 : 진행된 단계만 보상
    public void cancelOrderSaga(UUID orderUUID) {
        OrderSaga saga = orderSagaRepository.findByOrderUUID(orderUUID).orElse(null);
        if (saga == null || saga.getSagaStatus() != SagaStatus.IN_PROGRESS) {
            log.info("[Orchestrator] no saga to compensate : orderUUID={}", orderUUID);
            return;
        }

        log.info("[Orchestrator] cancel order saga : orderUUID={}", orderUUID);
        compensate(findOrderByUUID(orderUUID), saga, "사용자 주문 취소");
    }

    private void requestStockCheck(Order order, OrderSaga saga) {
        saga.requestStockQtyChecked();
        order.updateStatus(OrderStatus.INVENTORY_REQUESTED);

        sagaEventPublisher.publishStockCheckRequested(order.getOrderUUID(), order.getOrderItems());
    }

    // consume 재고 확인 응답
    public void handleStockCheckResponse(StockCheckResponseEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        if (!saga.isInProgressAt(SagaStep.STOCK)) {
            log.info("[Orchestrator] ignore stock check response : orderUUID={}, saga={}", orderUUID, saga.getSagaStatus());
            return;
        }

        Order order = findOrderByUUID(orderUUID);
        if (event.getAvailable()) {
            requestStockDeduction(order);
        } else {
            failStockSaga(order, saga, "재고 부족 (확인)");
        }
    }

    private void requestStockDeduction(Order order) {
        log.info("[Orchestrator] request stock deduction : orderUUID={}", order.getOrderUUID());
        sagaEventPublisher.publishStockDeductRequested(order.getOrderUUID(), order.getOrderItems());
    }

    // consume 단순 재고 차감 성공
    public void handleStockDeductSuccess(StockDeductEvent event) {
        confirmStockDeduction(UUID.fromString(event.getOrderUuid()));
    }

    // consume 배치 재고 차감 성공
    public void handleStockBulkDeducted(StockDeductEvent event) {
        confirmStockDeduction(UUID.fromString(event.getOrderUuid()));
    }

    private void confirmStockDeduction(UUID orderUUID) {
        log.info("[Orchestrator] Stock deduction success : orderUUID={}", orderUUID);
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        Order order = findOrderByUUID(orderUUID);

        // 취소 후 늦게 도착한 차감 결과 -> 복구
        if (saga.isCompensated() && saga.getProductStatus() == ProductStatus.PENDING) {
            saga.completeStockDeduction();
            saga.execCompensation("사용자 주문 취소");
            requestStockRestoration(order);
            return;
        }
        if (!saga.isInProgressAt(SagaStep.STOCK) || saga.getProductStatus() != ProductStatus.PENDING) {
            log.info("[Orchestrator] ignore stock deducted : orderUUID={}", orderUUID);
            return;
        }

        saga.completeStockDeduction();
        order.updateStatus(OrderStatus.INVENTORY_CONFIRMED);

        requestPayment(order, saga);
    }

    // consume 재고 부족 (단순 / 배치)
    public void handleStockLacked(StockDeductLackedEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        if (!saga.isInProgressAt(SagaStep.STOCK)) {
            log.info("[Orchestrator] ignore stock lacked : orderUUID={}", orderUUID);
            return;
        }

        failStockSaga(findOrderByUUID(orderUUID), saga, "재고 부족 (차감)");
    }

    private void failStockSaga(Order order, OrderSaga saga, String reason) {
        log.info("[Orchestrator] stock lacked : orderUUID={}", order.getOrderUUID());
        saga.failStockDeduction(reason);
        order.updateStatus(OrderStatus.CANCELLED);
    }

    // consume 재고 복구 완료
    public void handleStockRestored(StockRestoredEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        if (!saga.isCompensated() || !saga.needsStockRestoration()) {
            log.info("[Orchestrator] ignore stock restored : orderUUID={}", orderUUID);
            return;
        }

        log.info("[Orchestrator] stock restored : orderUUID={}", orderUUID);
        saga.completeStockRestoration();
        saga.completeCompensationIfDone();
    }

    private void requestPayment(Order order, OrderSaga saga) {
        log.info("[Orchestrator] request payment : orderUUID={}", order.getOrderUUID());
        saga.startPayment();
        order.updateStatus(OrderStatus.PAYMENT_REQUESTED);

        sagaEventPublisher.publishPaymentRequested(order.getOrderUUID(), order.getTotalPrice());
    }

    // consume 결제 성공
    public void handlePaymentSuccess(PaymentSuccessEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        log.info("[Orchestrator] payment success : orderUUID={}", orderUUID);
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        Order order = findOrderByUUID(orderUUID);

        // 취소 후 늦게 도착한 결제 성공 -> 결제 취소
        if (saga.isCompensated() && saga.getPaymentStatus() == PaymentStatus.PENDING) {
            saga.completePayment();
            saga.execCompensation("사용자 주문 취소");
            requestPaymentCancellation(order);
            return;
        }
        if (!saga.isInProgressAt(SagaStep.PAYMENT) || saga.getPaymentStatus() != PaymentStatus.PENDING) {
            log.info("[Orchestrator] ignore payment success : orderUUID={}", orderUUID);
            return;
        }

        saga.completePayment();
        order.updateStatus(OrderStatus.PAID);

        requestShipment(order, saga);
    }

    // consume 결제 실패
    public void handlePaymentFailed(PaymentFailedEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        log.info("[Orchestrator] payment failed : orderUUID={}, reason={}", orderUUID, event.getFailureReason());
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        if (saga.getPaymentStatus() != PaymentStatus.PENDING) {
            log.info("[Orchestrator] ignore payment failed : orderUUID={}", orderUUID);
            return;
        }

        saga.failPayment();
        if (saga.isCompensated()) {
            saga.completeCompensationIfDone();
            return;
        }
        if (saga.isInProgressAt(SagaStep.PAYMENT)) {
            compensate(findOrderByUUID(orderUUID), saga, "결제 실패");
        }
    }

    // consume 결제 취소 완료
    public void handlePaymentCancelled(PaymentCancelledEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        if (!saga.isCompensated() || !saga.needsPaymentCancellation()) {
            log.info("[Orchestrator] ignore payment cancelled : orderUUID={}", orderUUID);
            return;
        }

        log.info("[Orchestrator] payment cancelled : orderUUID={}", orderUUID);
        saga.completePaymentCancellation();
        saga.completeCompensationIfDone();
    }

    private void requestShipment(Order order, OrderSaga saga) {
        log.info("[Orchestrator] request shipment : orderUUID={}", order.getOrderUUID());
        saga.startShipment();

        sagaEventPublisher.publishShipmentRequested(order.getOrderUUID(), order.getUserId(), order.getAddress(), order.getContact(), order.getOrderItems());
    }

    // consume 배송 생성
    public void handleShipmentCreated(ShipmentCreatedEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        OrderSaga saga = findSagaByOrderUUID(orderUUID);
        if (!saga.isInProgressAt(SagaStep.SHIPMENT)) {
            log.info("[Orchestrator] ignore shipment created : orderUUID={}", orderUUID);
            return;
        }

        log.info("[Orchestrator] shipment created : orderUUID={}", orderUUID);
        saga.completeShipment();
        findOrderByUUID(orderUUID).updateStatus(OrderStatus.SHIPPED);
    }

    // consume 배송 실패
    public void handleShipmentFailed(ShipmentFailedEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        OrderSaga saga = findSagaByOrderUUID(orderUUID);

        // 배송 생성 후 실패는 자동 보상 대상 아님
        if (!saga.isInProgressAt(SagaStep.SHIPMENT)) {
            log.warn("[Orchestrator] shipment failed after created, manual handling required : orderUUID={}", orderUUID);
            return;
        }

        log.info("[Orchestrator] shipment failed : orderUUID={}", orderUUID);
        saga.failShipment();
        compensate(findOrderByUUID(orderUUID), saga, "배송 실패");
    }

    // consume 배송 완료
    public void handleShipmentCompleted(ShipmentCompletedEvent event) {
        UUID orderUUID = UUID.fromString(event.getOrderUuid());
        Order order = findOrderByUUID(orderUUID);
        if (order.getStatus() != OrderStatus.SHIPPED) {
            log.info("[Orchestrator] ignore shipment completed, order status={} : orderUUID={}", order.getStatus(), orderUUID);
            return;
        }

        log.info("[Orchestrator] shipment completed : orderUUID={}", orderUUID);
        order.updateStatus(OrderStatus.DELIVERED);
    }

    private void compensate(Order order, OrderSaga saga, String reason) {
        log.info("[Orchestrator] compensate : orderUUID={}, reason={}", order.getOrderUUID(), reason);
        saga.execCompensation(reason);
        if (order.getStatus() != OrderStatus.CANCELLED) {
            order.updateStatus(OrderStatus.CANCELLED);
        }

        if (saga.needsPaymentCancellation()) {
            requestPaymentCancellation(order);
        }
        if (saga.needsStockRestoration()) {
            requestStockRestoration(order);
        }
        saga.completeCompensationIfDone();
    }

    private void requestStockRestoration(Order order) {
        sagaEventPublisher.publishStockRestoreRequested(order.getOrderUUID(), order.getOrderItems());
    }

    private void requestPaymentCancellation(Order order) {
        sagaEventPublisher.publishPaymentCancellationRequested(order.getOrderUUID());
    }

    private OrderSaga findSagaByOrderUUID(UUID orderUUID) {
        return orderSagaRepository.findByOrderUUID(orderUUID)
                .orElseThrow(() -> new IllegalStateException("사가를 찾을 수 없습니다: " + orderUUID));
    }

    private Order findOrderByUUID(UUID orderUUID) {
        return orderRepository.findByOrderUUID(orderUUID)
                .orElseThrow(() -> new IllegalStateException("주문을 찾을 수 없습니다: " + orderUUID));
    }

}
