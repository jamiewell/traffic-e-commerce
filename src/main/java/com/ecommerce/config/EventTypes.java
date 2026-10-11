package com.ecommerce.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

// doc/kafka-convention.md
public final class EventTypes {

    private EventTypes() {
    }

    public static final String HEADER_EVENT_TYPE = "eventType";
    public static final String HEADER_EVENT_ID = "eventId";

    // 주문
    public static final String ORDER_CREATED = "order.created";
    public static final String ORDER_CANCELLED = "order.cancelled";

    // 재고 요청
    public static final String STOCK_CHECK = "stock.check";
    public static final String STOCK_DEDUCT = "stock.deduct";
    public static final String STOCK_BULK_DEDUCT = "stock.bulk.deduct";
    public static final String STOCK_RESTORE = "stock.restore";

    // 재고 결과
    public static final String STOCK_CHECK_RESPONSE = "stock.check.response";
    public static final String STOCK_DEDUCTED = "stock.deducted";
    public static final String STOCK_LACKED = "stock.lacked";
    public static final String STOCK_BULK_DEDUCTED = "stock.bulk.deducted";
    public static final String STOCK_BULK_LACKED = "stock.bulk.lacked";
    public static final String STOCK_RESTORED = "stock.restored";

    // 결제 요청
    public static final String PAYMENT_REQUEST = "payment.request";
    public static final String PAYMENT_CANCEL = "payment.cancel";

    // 결제 결과
    public static final String PAYMENT_SUCCEEDED = "payment.succeeded";
    public static final String PAYMENT_FAILED = "payment.failed";
    public static final String PAYMENT_CANCELLED = "payment.cancelled";

    // 배송 요청
    public static final String SHIPMENT_REQUEST = "shipment.request";

    // 배송 결과
    public static final String SHIPMENT_CREATED = "shipment.created";
    public static final String SHIPMENT_CHANGED = "shipment.changed";
    public static final String SHIPMENT_FAILED = "shipment.failed";
    public static final String SHIPMENT_COMPLETED = "shipment.completed";
    public static final String SHIPMENT_CANCELLED = "shipment.cancelled";


    // 헤더 없으면 key 사용 (전 도메인 전환 후 제거)
    public static String resolveEventType(ConsumerRecord<String, ?> record) {
        Header header = record.headers().lastHeader(HEADER_EVENT_TYPE);
        if (header != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return record.key() == null ? "" : record.key();
    }

}
