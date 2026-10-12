package com.ecommerce.product.productService;

import com.ecommerce.member.memberEntity.Users;
import com.ecommerce.member.memberRepository.UserRepository;
import com.ecommerce.product.dto.request.ProductModifyRequest;
import com.ecommerce.product.dto.request.ProductRegisterRequest;
import com.ecommerce.product.dto.request.StockDeductRequest;
import com.ecommerce.product.dto.response.ProductResponse;
import com.ecommerce.product.dto.response.StockDeductResult;
import com.ecommerce.product.dto.response.StockRestoreResult;
import com.ecommerce.product.dto.search.DetailedSearchCondition;
import com.ecommerce.product.event.ActiveStockEventPublisher;
import com.ecommerce.product.productEntity.Product;
import com.ecommerce.product.productEntity.ProductCategory;
import com.ecommerce.product.productEntity.ProductImage;
import com.ecommerce.product.productEntity.StockDeductHistory;
import com.ecommerce.product.productRepository.ProductCategoryRepository;
import com.ecommerce.product.productRepository.ProductImageRepository;
import com.ecommerce.product.productRepository.ProductRepository;
import com.ecommerce.product.productRepository.StockDeductHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {

    private final UserRepository userRepository;
    private final ProductRepository productRepository;
    private final ProductCategoryRepository categoryRepository;
    private final ProductImageRepository imageRepository;
    private final StockDeductHistoryRepository historyRepository;
    private final ActiveStockEventPublisher eventPublisher;

    // 판매자: 전체 상품 조회
    public List<ProductResponse> getSellerProducts(UUID sellerId) {
        List<Product> sellerProducts = productRepository.findAllBySellerId(sellerId);

        return sellerProducts.stream().map(ProductResponse::from).collect(Collectors.toList());
    }

    // 판매자: 상태값을 필터링해서 상품 조회
    public List<ProductResponse> getSellerProducts(UUID sellerId, boolean isActive) {
        List<Product> sellerProducts = productRepository.findBySellerIdAndIsActive(sellerId, isActive);

        return sellerProducts.stream().map(ProductResponse::from).collect(Collectors.toList());
    }

    // 판매자: 상품 최초 등록
    public Product registerProduct(String sellerId, ProductRegisterRequest registerRequest) {
        // 판매자 정보 조회
        Users seller = userRepository.findByUserId(UUID.fromString(sellerId))
                .orElseThrow(() -> new IllegalArgumentException("Invalid user"));

        // 카테고리 정보 조회
        ProductCategory category = categoryRepository.findByName(registerRequest.getCategory())
                .orElseGet(() -> categoryRepository.save(ProductCategory.of(registerRequest.getCategory())));

        Product product = Product.of(seller.userId, registerRequest, category);
        Product savedProduct = productRepository.save(product);

        List<ProductImage> imageList = registerRequest.getImageUrls().stream()
                .map(url -> ProductImage.of(savedProduct, url))
                .toList();

        imageRepository.saveAll(imageList);

        return Product.of(savedProduct, imageList);
    }

    // 판매자: 등록된 상품 정보 변경
    @Transactional
    public Product modifyProduct(String sellerId, String productId, ProductModifyRequest modifyRequest) {
        Product product = productRepository.findByProductUUID(UUID.fromString(productId))
                .filter(p -> p.getSellerId().equals(sellerId))
                .orElseThrow(() -> new IllegalArgumentException("Cannot find products"));

        // 기존 이미지 소프트 딜리트 처리
        List<ProductImage> images = imageRepository.findByProductAndIsDeletedFalse(product);
        images.forEach(ProductImage::softDelete);

        // 새 이미지들 저장
        List<ProductImage> newImages = modifyRequest.getImageUrls().stream()
                .map(url -> ProductImage.builder()
                        .product(product)
                        .imageUrl(url)
                        .isActive(true)      // 활성 상태
                        .isDeleted(false)    // 삭제 안된 상태
                        .build())
                .toList();

        imageRepository.saveAll(newImages);

        // 상품 이미지 초기화
        List<ProductImage> imageList = modifyRequest.getImageUrls().stream()
                .map(url -> ProductImage.of(product, url))
                .toList();

        // 카테고리 정보 변경
        categoryRepository.save(ProductCategory.of(modifyRequest.getCategory()));

        // 상품 정보 업데이트
        Product modifiedProduct = Product.of(product, imageList);
        return productRepository.save(modifiedProduct);
    }

    // 판매자: 등록된 상품 삭제(소프트 삭제)
    public void deleteProduct(String sellerId, String productId) {
        // 판매자 정보 조회
        Users seller = userRepository.findByUserId(UUID.fromString(sellerId))
                .orElseThrow(() -> new IllegalArgumentException("Invalid user"));


        Product product = productRepository.findByProductUUID(UUID.fromString(productId))
                .filter(p -> p.getSellerId().equals(seller.userId))
                .orElseThrow(() -> new IllegalArgumentException("No products"));

        // 상품 소프트삭제
        product.softDelete();
        productRepository.save(product);
    }


    // 구매자: 아이템 조회
    public ProductResponse getProductDetail(String productId) {
        var detailedProduct = productRepository.findByProductUUID(UUID.fromString(productId))
                .filter(p -> p.isActive() && !p.isDeleted())
                .orElseThrow(() -> new IllegalArgumentException("There are no products or deleted"));

        return ProductResponse.from(detailedProduct);
    }

    // 구매자: 상세 아이템 조회
    public Page<ProductResponse> searchDetailForCustomer(DetailedSearchCondition condition) {
        Page<Product> searchedProduct = productRepository.searchProductForCustomer(
                condition.getName()
                , condition.getCategoryId()
                , condition.getMinPrice()
                , condition.getMaxPrice()
                , condition.getPageable());

        return searchedProduct.map(ProductResponse::from);

    }


    // 재고 확인 : 예약하지 않음, 실제 보장은 차감 시 락 안에서 재검사
    @Transactional(readOnly = true)
    public void hasStockActive(List<StockDeductRequest> requests, UUID orderUUID) {
        List<StockDeductResult> results = mergeByProduct(requests).stream()
                .map(request -> checkStock(productRepository.findByProductUUID(request.getProductUUID()).orElse(null), request))
                .toList();

        eventPublisher.publishStockActive(orderUUID, results);
    }

    // 단일 상품 재고 차감
    @Transactional
    public void deductStock(UUID productUUID, int quantity, UUID orderUUID) {
        log.info("Request deduce qty: productUUID={}, qty={}, orderUUID={}", productUUID, quantity, orderUUID);
        if (isDeductedOrder(orderUUID)) {
            return;
        }

        // 비관적 락으로 상품 조회
        Product product = productRepository.findByProductUUIDWithLock(productUUID).orElse(null);
        StockDeductResult checked = checkStock(product, new StockDeductRequest(productUUID, quantity));

        if (checked.hasFailed()) {
            eventPublisher.publishStockLacked(orderUUID, productUUID, quantity, checked.getRemainingStock());
            log.warn("Stock lacked: productUUID={}, requested qty={}, current qty={}", productUUID, quantity, checked.getRemainingStock());
            return;
        }

        StockDeductResult result = deductAndRecord(product, quantity, orderUUID);
        eventPublisher.publishStockDeduct(orderUUID, productUUID, quantity, result.getRemainingStock());
        log.info("Success deduct stock: productUUID={}, remainingQty={}", productUUID, result.getRemainingStock());
    }

    // 여러 상품 재고 차감 : 전부 확인 후 차감 (하나라도 부족하면 차감 없음)
    @Transactional
    public void deductStocks(List<StockDeductRequest> requests, UUID orderUUID) {
        if (isDeductedOrder(orderUUID)) {
            return;
        }

        List<StockDeductRequest> mergedRequests = mergeByProduct(requests);
        Map<UUID, Product> productMap = findProductMapWithLock(mergedRequests.stream().map(StockDeductRequest::getProductUUID).toList());

        List<StockDeductResult> checkedList = mergedRequests.stream()
                .map(request -> checkStock(productMap.get(request.getProductUUID()), request))
                .toList();

        if (checkedList.stream().anyMatch(StockDeductResult::hasFailed)) {
            eventPublisher.publishBulkStockFailed(orderUUID, checkedList);
            return;
        }

        List<StockDeductResult> results = mergedRequests.stream()
                .map(request -> deductAndRecord(productMap.get(request.getProductUUID()), request.getQuantity(), orderUUID))
                .toList();
        eventPublisher.publishBulkStockDeduct(orderUUID, results);
    }

    // 재고 복구 : 차감 이력 기준, 이미 복구된 이력은 제외
    @Transactional
    public void restoreStocks(UUID orderUUID) {
        List<StockDeductHistory> historyList = historyRepository.findByOrderUUIDAndIsRestoredFalse(orderUUID);
        Map<UUID, Product> productMap = findProductMapWithLock(historyList.stream().map(StockDeductHistory::getProductUUID).toList());

        List<StockRestoreResult> results = new ArrayList<>();
        for (StockDeductHistory history : historyList) {
            Product product = productMap.get(history.getProductUUID());
            if (product == null) {
                throw new IllegalStateException("cannot find product: " + history.getProductUUID());
            }

            product.restoreStock(history.getQuantity());
            history.restore();
            results.add(new StockRestoreResult(history.getProductUUID(), history.getQuantity(), product.getStockQuantity()));
        }

        eventPublisher.publishStockRestored(orderUUID, results);
    }

    private boolean isDeductedOrder(UUID orderUUID) {
        if (historyRepository.existsByOrderUUID(orderUUID)) {
            log.info("Skip duplicated deduction: orderUUID={}", orderUUID);
            return true;
        }
        return false;
    }

    private StockDeductResult checkStock(Product product, StockDeductRequest request) {
        if (product == null) {
            return StockDeductResult.failed(request.getProductUUID(), request.getQuantity(), 0);
        }
        if (!product.canDeductStock(request.getQuantity())) {
            return StockDeductResult.failed(request.getProductUUID(), request.getQuantity(), product.getStockQuantity());
        }
        return StockDeductResult.success(request.getProductUUID(), request.getQuantity(), product.getStockQuantity());
    }

    private StockDeductResult deductAndRecord(Product product, int quantity, UUID orderUUID) {
        product.deductStock(quantity);
        historyRepository.save(StockDeductHistory.of(orderUUID, product.getProductUUID(), quantity));
        return StockDeductResult.success(product.getProductUUID(), quantity, product.getStockQuantity());
    }

    // 데드락 방지를 위해 정렬 후 락
    private Map<UUID, Product> findProductMapWithLock(List<UUID> productUUIDs) {
        if (productUUIDs.isEmpty()) {
            return Map.of();
        }
        List<UUID> sortedUUIDs = productUUIDs.stream().distinct().sorted().toList();
        return productRepository.findByProductUUIDsWithLock(sortedUUIDs).stream()
                .collect(Collectors.toMap(Product::getProductUUID, Function.identity()));
    }

    // 같은 상품이 여러 줄이면 수량 합산
    private List<StockDeductRequest> mergeByProduct(List<StockDeductRequest> requests) {
        Map<UUID, Integer> quantityMap = requests.stream()
                .collect(Collectors.toMap(StockDeductRequest::getProductUUID, StockDeductRequest::getQuantity, Integer::sum, LinkedHashMap::new));
        return quantityMap.entrySet().stream()
                .map(entry -> new StockDeductRequest(entry.getKey(), entry.getValue()))
                .toList();
    }

}
