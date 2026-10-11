package com.ecommerce.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers[0]}")
    private String bootStrapServers;

    @Bean
    public ProducerFactory<String, byte[]> producerFactory() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        configMap.put(ProducerConfig.ACKS_CONFIG, "all");  // 모든 복제본이 메시지를 받았는지 확인
        configMap.put(ProducerConfig.RETRIES_CONFIG, 3);   // 실패 시 재시도 횟수
        configMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // 재시도 간격

        return new DefaultKafkaProducerFactory<>(configMap);
    }

    @Bean
    public KafkaTemplate<String, byte[]> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // groupId 는 @KafkaListener 에서 지정
    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(configMap);
    }

    // 수동 ack, 예외 시 1초 간격 2회 재시도 후 skip
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2L)));

        return factory;
    }

}
