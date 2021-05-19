package com.wellsfargo.cto.eai.kafkastarter.producer;

import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class CustomerProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendAvroData(com.wellsfargo.cto.eai.kafkastarter.Customer customer) {
        this.kafkaTemplate.send("wf-concurrent-customer", customer.getId(), customer);
    }

}


