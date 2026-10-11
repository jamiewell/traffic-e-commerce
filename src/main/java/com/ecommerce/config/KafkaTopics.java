package com.ecommerce.config;

// doc/kafka-convention.md
public final class KafkaTopics {

    private KafkaTopics() {
    }

    // 결과 토픽
    public static final String ORDER_EVENTS = "order-events";
    public static final String PRODUCT_EVENTS = "product-events";
    public static final String PAYMENT_EVENTS = "payment-events";
    public static final String SHIPMENT_EVENTS = "shipment-events";

    // 요청 토픽
    public static final String PRODUCT_COMMANDS = "product-commands";
    public static final String PAYMENT_COMMANDS = "payment-commands";
    public static final String SHIPMENT_COMMANDS = "shipment-commands";

    // 컨슈머 그룹
    public static final String ORDER_SERVICE_GROUP = "order-service";
    public static final String PRODUCT_SERVICE_GROUP = "product-service";
    public static final String PAYMENT_SERVICE_GROUP = "payment-service";
    public static final String SHIPMENT_SERVICE_GROUP = "shipment-service";

}
