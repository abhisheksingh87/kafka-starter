package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.Customer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Getter
@Setter
@Service
public class CustomerErrorConsumer {

    private Customer customer;

    @KafkaListener(topics = "wf-customer", groupId = "error-customer", errorHandler = "customerConsumerErrorHandler")
    public void consume(Customer customer) {
        if (customer.getId().equals("23")) {
            throw new RuntimeException("Invalid Message");
        }
        log.info("Message: {}", customer);
    }

}
