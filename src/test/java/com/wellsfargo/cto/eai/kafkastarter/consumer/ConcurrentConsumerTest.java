package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.producer.CustomerProducer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.*;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
public class ConcurrentConsumerTest {

    @Autowired
    private CustomerProducer customerProducer;

    @Autowired
    private ConcurrentCustomerConsumer concurrentCustomerConsumer;

    @Test
    public void testAvroData() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //given
        com.wellsfargo.cto.eai.kafkastarter.Customer customer = com.wellsfargo.cto.eai.kafkastarter.Customer.newBuilder()
                .setId("1")
                .setFirstName("alex")
                .setLastName("smith")
                .setPhoneNumber("424645290").build();

        customerProducer.sendAvroData(customer);
        customerProducer.sendAvroData(customer);
        countDownLatch.await(15, TimeUnit.SECONDS);


        //then
        assertThat(concurrentCustomerConsumer.getCustomer()).usingRecursiveComparison().isEqualTo(customer);
    }

    @Configuration
    @EnableKafka
    static class KafkaConfiguration {

        @Bean
        public KafkaProperties kafkaProperties() {
            return new KafkaProperties();
        }

        @Bean
        public ConcurrentCustomerConsumer customerConsumer() {
         return new ConcurrentCustomerConsumer();
        }

        @Bean
        public ProducerFactory<String, Object> producerFactory() {
            Map<String, Object> kafkaProperties = kafkaProperties().buildProducerProperties();
            kafkaProperties.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, "180000");
            kafkaProperties.put("bootstrap.servers", "localhost:9092");
            kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            kafkaProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            return new DefaultKafkaProducerFactory<>(kafkaProperties);
        }

        @Bean
        public NewTopic createTopic() {
            return TopicBuilder.name("wf-concurrent-customer").partitions(3).replicas(1).build();
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate(){
            return new KafkaTemplate<>(producerFactory());
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerConfigs()));
            factory.setConcurrency(1);
            return factory;
        }

        @Bean
        public KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
            return new KafkaListenerEndpointRegistry();
        }

        @Bean
        public Map<String, Object> consumerConfigs() {
            Map<String, Object> kafkaProperties = new HashMap<>();
            kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-consumer");
            kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            kafkaProperties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
            kafkaProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            return kafkaProperties;
        }

        @Bean
        public CustomerProducer customerProducer() {
            return new CustomerProducer(kafkaTemplate());
        }

    }
}
