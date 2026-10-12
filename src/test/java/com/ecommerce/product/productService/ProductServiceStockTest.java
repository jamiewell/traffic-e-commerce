package com.ecommerce.product.productService;

import com.ecommerce.member.memberRepository.UserRepository;
import com.ecommerce.product.dto.request.StockDeductRequest;
import com.ecommerce.product.dto.response.StockDeductResult;
import com.ecommerce.product.dto.response.StockRestoreResult;
import com.ecommerce.product.event.ActiveStockEventPublisher;
import com.ecommerce.product.productEntity.Product;
import com.ecommerce.product.productEntity.StockDeductHistory;
import com.ecommerce.product.productRepository.ProductCategoryRepository;
import com.ecommerce.product.productRepository.ProductImageRepository;
import com.ecommerce.product.productRepository.ProductRepository;
import com.ecommerce.product.productRepository.StockDeductHistoryRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductServiceStockTest {

    @Mock
    private UserRepository userRepository;
    @Mock
    private ProductRepository productRepository;
    @Mock
    private ProductCategoryRepository categoryRepository;
    @Mock
    private ProductImageRepository imageRepository;
    @Mock
    private StockDeductHistoryRepository historyRepository;
    @Mock
    private ActiveStockEventPublisher eventPublisher;
    @InjectMocks
    private ProductService productService;

    @Captor
    private ArgumentCaptor<List<StockDeductResult>> resultCaptor;
    @Captor
    private ArgumentCaptor<List<StockRestoreResult>> restoreCaptor;

    private final UUID orderUUID = UUID.randomUUID();

    private Product givenProduct(int stockQuantity) {
        return Product.builder()
                .productUUID(UUID.randomUUID())
                .name("item")
                .price(new BigDecimal("1000"))
                .stockQuantity(stockQuantity)
                .build();
    }

    @Test
    @DisplayName("stock.check : 재고 충분 -> available")
    void hasStockActive_available() {
        Product product = givenProduct(10);
        given(productRepository.findByProductUUID(product.getProductUUID())).willReturn(Optional.of(product));

        productService.hasStockActive(List.of(new StockDeductRequest(product.getProductUUID(), 3)), orderUUID);

        verify(eventPublisher).publishStockActive(eq(orderUUID), resultCaptor.capture());
        then(resultCaptor.getValue()).allMatch(StockDeductResult::isSuccess);
        then(product.getStockQuantity()).isEqualTo(10);
    }

    @Test
    @DisplayName("stock.check : 재고 부족 / 없는 상품 / 판매중지 -> 부족 상품 포함")
    void hasStockActive_lacked() {
        Product lacked = givenProduct(1);
        Product inactive = givenProduct(10);
        inactive.softDelete();
        UUID missingUUID = UUID.randomUUID();
        given(productRepository.findByProductUUID(lacked.getProductUUID())).willReturn(Optional.of(lacked));
        given(productRepository.findByProductUUID(inactive.getProductUUID())).willReturn(Optional.of(inactive));
        given(productRepository.findByProductUUID(missingUUID)).willReturn(Optional.empty());

        productService.hasStockActive(List.of(
                new StockDeductRequest(lacked.getProductUUID(), 2),
                new StockDeductRequest(inactive.getProductUUID(), 1),
                new StockDeductRequest(missingUUID, 1)), orderUUID);

        verify(eventPublisher).publishStockActive(eq(orderUUID), resultCaptor.capture());
        then(resultCaptor.getValue()).noneMatch(StockDeductResult::isSuccess);
        then(resultCaptor.getValue().get(0).getRequestedQuantity()).isEqualTo(2);
        then(resultCaptor.getValue().get(0).getRemainingStock()).isEqualTo(1);
    }

    @Test
    @DisplayName("stock.deduct : 성공 -> 차감, 이력 저장, stock.deducted")
    void deductStock_success() {
        Product product = givenProduct(5);
        given(productRepository.findByProductUUIDWithLock(product.getProductUUID())).willReturn(Optional.of(product));

        productService.deductStock(product.getProductUUID(), 2, orderUUID);

        then(product.getStockQuantity()).isEqualTo(3);
        verify(historyRepository).save(any(StockDeductHistory.class));
        verify(eventPublisher).publishStockDeduct(orderUUID, product.getProductUUID(), 2, 3);
    }

    @Test
    @DisplayName("stock.deduct : 부족 -> 차감 없음, stock.lacked")
    void deductStock_lacked() {
        Product product = givenProduct(1);
        given(productRepository.findByProductUUIDWithLock(product.getProductUUID())).willReturn(Optional.of(product));

        productService.deductStock(product.getProductUUID(), 2, orderUUID);

        then(product.getStockQuantity()).isEqualTo(1);
        verify(historyRepository, never()).save(any());
        verify(eventPublisher).publishStockLacked(orderUUID, product.getProductUUID(), 2, 1);
    }

    @Test
    @DisplayName("stock.deduct 중복 수신 -> 무시")
    void deductStock_duplicate() {
        given(historyRepository.existsByOrderUUID(orderUUID)).willReturn(true);

        productService.deductStock(UUID.randomUUID(), 1, orderUUID);

        verifyNoInteractions(productRepository, eventPublisher);
    }

    @Test
    @DisplayName("stock.bulk.deduct : 하나라도 부족 -> 전체 차감 없음, stock.bulk.lacked")
    void deductStocks_lacked() {
        Product enough = givenProduct(10);
        Product lacked = givenProduct(1);
        given(productRepository.findByProductUUIDsWithLock(anyList())).willReturn(List.of(enough, lacked));

        productService.deductStocks(List.of(
                new StockDeductRequest(enough.getProductUUID(), 2),
                new StockDeductRequest(lacked.getProductUUID(), 3)), orderUUID);

        then(enough.getStockQuantity()).isEqualTo(10);
        then(lacked.getStockQuantity()).isEqualTo(1);
        verify(historyRepository, never()).save(any());
        verify(eventPublisher).publishBulkStockFailed(eq(orderUUID), resultCaptor.capture());
        then(resultCaptor.getValue()).filteredOn(StockDeductResult::hasFailed).hasSize(1);
    }

    @Test
    @DisplayName("stock.bulk.deduct : 전부 충분 -> 전체 차감, stock.bulk.deducted (같은 상품은 합산)")
    void deductStocks_success() {
        Product first = givenProduct(10);
        Product second = givenProduct(5);
        given(productRepository.findByProductUUIDsWithLock(anyList())).willReturn(List.of(first, second));

        productService.deductStocks(List.of(
                new StockDeductRequest(first.getProductUUID(), 2),
                new StockDeductRequest(second.getProductUUID(), 5),
                new StockDeductRequest(first.getProductUUID(), 1)), orderUUID);

        then(first.getStockQuantity()).isEqualTo(7);
        then(second.getStockQuantity()).isEqualTo(0);
        verify(historyRepository, times(2)).save(any(StockDeductHistory.class));
        verify(eventPublisher).publishBulkStockDeduct(eq(orderUUID), resultCaptor.capture());
        then(resultCaptor.getValue()).extracting(StockDeductResult::getRequestedQuantity).containsExactly(3, 5);
    }

    @Test
    @DisplayName("stock.restore : 미복구 이력만 복구, stock.restored")
    void restoreStocks() {
        Product product = givenProduct(3);
        StockDeductHistory history = StockDeductHistory.of(orderUUID, product.getProductUUID(), 2);
        given(historyRepository.findByOrderUUIDAndIsRestoredFalse(orderUUID)).willReturn(List.of(history));
        given(productRepository.findByProductUUIDsWithLock(anyList())).willReturn(List.of(product));

        productService.restoreStocks(orderUUID);

        then(product.getStockQuantity()).isEqualTo(5);
        then(history.isRestored()).isTrue();
        verify(eventPublisher).publishStockRestored(eq(orderUUID), restoreCaptor.capture());
        then(restoreCaptor.getValue()).singleElement()
                .satisfies(result -> then(result.getRestoredQuantity()).isEqualTo(2));
    }

    @Test
    @DisplayName("stock.restore 중복 수신 -> 복구 없음, 빈 stock.restored")
    void restoreStocks_duplicate() {
        given(historyRepository.findByOrderUUIDAndIsRestoredFalse(orderUUID)).willReturn(List.of());

        productService.restoreStocks(orderUUID);

        verifyNoInteractions(productRepository);
        verify(eventPublisher).publishStockRestored(eq(orderUUID), restoreCaptor.capture());
        then(restoreCaptor.getValue()).isEmpty();
    }

}
