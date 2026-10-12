package com.ecommerce.product.dto.response;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.UUID;

@Getter
@AllArgsConstructor
public class StockRestoreResult {

    private UUID productUUID;

    private Integer restoredQuantity;

    private Integer currentStock;

}
