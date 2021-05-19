package com.wellsfargo.cto.eai.kafkastarter.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.var;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class KafkaConfiguration {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Value("${backOffPeriod}")
    private long backOffPeriod;

    @Value("${retryAttempts}")
    private int retryAttemps;

    @Value("${interval}")
    private long interval;

    @Bean
    public ConsumerFactory<Object, Object> consumerFactory() {
        var properties = kafkaProperties.buildConsumerProperties();
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "180000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        var properties = kafkaProperties.buildProducerProperties();
        properties.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, "180000");
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public KafkaOperations<String, Object> kafkaOperations() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


    @Bean
    public RetryTemplate createRetryTemplate() {
        var retryTemplate = new RetryTemplate();
        var retryPolicy = new SimpleRetryPolicy(retryAttemps);
        retryTemplate.setRetryPolicy(retryPolicy);
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(backOffPeriod);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> retryContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer concurrentKafkaListenerContainerFactoryConfigurer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        concurrentKafkaListenerContainerFactoryConfigurer.configure(containerFactory, consumerFactory());
        containerFactory.setRetryTemplate(createRetryTemplate());
        return containerFactory;
    }

    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name("wf-customer").partitions(2).replicas(1).build();
    }
}
