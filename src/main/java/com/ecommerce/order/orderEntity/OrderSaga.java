package com.ecommerce.order.orderEntity;

import com.ecommerce.order.status.*;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "order_sagas")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderSaga {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_uuid", columnDefinition = "BINARY(16)", unique = true, nullable = false)
    private UUID orderUUID;

    @Enumerated(EnumType.STRING)
    @Column(name = "saga_status", nullable = false)
    private SagaStatus sagaStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_step", nullable = false)
    private SagaStep currentStep;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_status")
    private PaymentStatus paymentStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "product_status")
    private ProductStatus productStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "shipment_status")
    private ShipmentStatus shipmentStatus;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    @Column(name = "started_at", nullable = false)
    private LocalDateTime startedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "failed_at")
    private LocalDateTime failedAt;

    @PrePersist
    protected void onCreate() {
        if (startedAt == null) startedAt = LocalDateTime.now();
    }

    public void requestStockQtyChecked() {
        this.currentStep = SagaStep.STOCK;
        this.sagaStatus = SagaStatus.IN_PROGRESS;
    }

    public void completeStockDeduction() {
        this.productStatus = ProductStatus.DEDUCTED;
    }

    // 재고 부족 : 보상할 것이 없으므로 바로 실패 종료
    public void failStockDeduction(String reason) {
        this.productStatus = ProductStatus.INSUFFICIENT;
        this.sagaStatus = SagaStatus.FAILED;
        this.failureReason = reason;
        this.failedAt = LocalDateTime.now();
    }

    public void startPayment() {
        this.currentStep = SagaStep.PAYMENT;
    }

    public void completePayment() {
        this.paymentStatus = PaymentStatus.COMPLETED;
    }

    public void failPayment() {
        this.paymentStatus = PaymentStatus.FAILED;
    }

    public void startShipment() {
        this.currentStep = SagaStep.SHIPMENT;
    }

    public void completeShipment() {
        this.shipmentStatus = ShipmentStatus.CREATED;
        this.sagaStatus = SagaStatus.COMPLETED;
        this.completedAt = LocalDateTime.now();
    }

    public void failShipment() {
        this.shipmentStatus = ShipmentStatus.FAILED;
    }

    public void execCompensation(String reason) {
        this.sagaStatus = SagaStatus.COMPENSATING;
        if (this.failureReason == null) {
            this.failureReason = reason;
            this.failedAt = LocalDateTime.now();
        }
    }

    public void completeStockRestoration() {
        this.productStatus = ProductStatus.RESTORED;
    }

    public void completePaymentCancellation() {
        this.paymentStatus = PaymentStatus.CANCELLED;
    }

    public boolean needsStockRestoration() {
        return this.productStatus == ProductStatus.DEDUCTED;
    }

    public boolean needsPaymentCancellation() {
        return this.paymentStatus == PaymentStatus.COMPLETED;
    }

    public void completeCompensationIfDone() {
        if (this.sagaStatus == SagaStatus.COMPENSATING && !needsStockRestoration() && !needsPaymentCancellation()) {
            this.sagaStatus = SagaStatus.COMPENSATED;
            this.completedAt = LocalDateTime.now();
        }
    }

    public boolean isInProgressAt(SagaStep step) {
        return this.sagaStatus == SagaStatus.IN_PROGRESS && this.currentStep == step;
    }

    // 취소/보상 진행 중이거나 보상 완료
    public boolean isCompensated() {
        return this.sagaStatus == SagaStatus.COMPENSATING || this.sagaStatus == SagaStatus.COMPENSATED;
    }

}
