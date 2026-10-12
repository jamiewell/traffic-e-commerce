package com.ecommerce.product.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

@Getter
@AllArgsConstructor
@Builder
public class StockDeductResult {

    private UUID productUUID;

    private boolean success;

    private Integer requestedQuantity;

    // 성공 : 차감 후 재고, 실패 : 현재 재고
    private Integer remainingStock;



    public static StockDeductResult success(UUID productUUID, Integer requestedQuantity, Integer remainingStock) {
        return StockDeductResult.builder()
                .productUUID(productUUID)
                .success(true)
                .requestedQuantity(requestedQuantity)
                .remainingStock(remainingStock)
                .build();
    }

    public static StockDeductResult failed(UUID productUUID, Integer requestedQuantity, Integer remainingStock) {
        return StockDeductResult.builder()
                .productUUID(productUUID)
                .success(false)
                .requestedQuantity(requestedQuantity)
                .remainingStock(remainingStock)
                .build();
    }

    public boolean hasFailed() {
        return !this.success;
    }

}
