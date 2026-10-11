package com.ecommerce.order.status;

public enum SagaStep {
    STOCK,          // 재고 확인/차감 단계
    PAYMENT,        // 결제 단계
    SHIPMENT        // 배송 단계
}
