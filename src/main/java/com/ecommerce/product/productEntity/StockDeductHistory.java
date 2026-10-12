package com.ecommerce.product.productEntity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

// 주문별 재고 차감 이력 : 중복 차감 방지, 복구 수량 기준
@Entity
@Table(name = "stock_deduct_history", indexes = @Index(name = "idx_stock_deduct_history_order", columnList = "order_uuid"))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class StockDeductHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_uuid", columnDefinition = "BINARY(16)", nullable = false)
    private UUID orderUUID;

    @Column(name = "product_uuid", columnDefinition = "BINARY(16)", nullable = false)
    private UUID productUUID;

    @Column(nullable = false)
    private Integer quantity;

    @Builder.Default
    @Column(name = "is_restored", nullable = false)
    private boolean isRestored = false;

    @Column(name = "crea_dt")
    private LocalDateTime creaDt;

    @Column(name = "updt_dt")
    private LocalDateTime updtDt;

    public static StockDeductHistory of(UUID orderUUID, UUID productUUID, int quantity) {
        return StockDeductHistory.builder()
                .orderUUID(orderUUID)
                .productUUID(productUUID)
                .quantity(quantity)
                .build();
    }

    public void restore() {
        this.isRestored = true;
    }

    @PrePersist
    protected void onCreate() {
        this.creaDt = LocalDateTime.now();
        this.updtDt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updtDt = LocalDateTime.now();
    }

}
