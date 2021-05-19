package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.Customer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Getter
@Setter
public class ConcurrentCustomerConsumer {

    private Customer customer;

    @KafkaListener(topics = "wf-concurrent-customer", groupId = "customer-consumer", concurrency = "3")
    public void consume(ConsumerRecord<String, Customer> consumerRecord) {
        log.info("Partition: {}, Offset: {}, Message: {}", consumerRecord.partition(),
                consumerRecord.offset(), consumerRecord.value());
        setCustomer(consumerRecord.value());
    }

}
