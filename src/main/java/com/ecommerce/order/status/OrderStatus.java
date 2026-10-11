package com.ecommerce.order.status;

public enum OrderStatus {
    ORDERED               // 주문 생성
    , INVENTORY_REQUESTED // 재고 확인 요청
    , INVENTORY_CONFIRMED // 재고 차감 완료
    , PAYMENT_REQUESTED   // 결제 요청
    , PAID                // 결제 완료
    , SHIPPED             // 배송 생성 (출고 준비)
    , DELIVERED           // 배송 완료
    , CANCELLED;          // 주문 취소 (최종)

    public boolean canTransitionTo(OrderStatus newStatus) {
        return switch (this) {
            case ORDERED -> newStatus == INVENTORY_REQUESTED || newStatus == CANCELLED;
            case INVENTORY_REQUESTED -> newStatus == INVENTORY_CONFIRMED || newStatus == CANCELLED;
            case INVENTORY_CONFIRMED -> newStatus == PAYMENT_REQUESTED || newStatus == CANCELLED;
            case PAYMENT_REQUESTED -> newStatus == PAID || newStatus == CANCELLED;
            case PAID -> newStatus == SHIPPED || newStatus == CANCELLED;
            case SHIPPED -> newStatus == DELIVERED;
            case DELIVERED, CANCELLED -> false;
        };
    }

    // PAID 시점에 배송 요청이 나가므로 그 전까지만
    public boolean isCancellableByUser() {
        return this == ORDERED || this == INVENTORY_REQUESTED
                || this == INVENTORY_CONFIRMED || this == PAYMENT_REQUESTED;
    }
}
