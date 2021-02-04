package com.wellsfargo.cto.eai.kafkastarter.producer;

import com.wellsfargo.cto.eai.kafkastarter.model.Customer;
import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class CustomerProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendData(Customer customer) {
        this.kafkaTemplate.sendDefault(customer.getId(), customer);
    }

}


