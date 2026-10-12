package com.ecommerce.product.productRepository;

import com.ecommerce.product.productEntity.StockDeductHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface StockDeductHistoryRepository extends JpaRepository<StockDeductHistory, Long> {

    boolean existsByOrderUUID(UUID orderUUID);

    List<StockDeductHistory> findByOrderUUIDAndIsRestoredFalse(UUID orderUUID);

}
