package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.model.Customer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
@Slf4j
@Getter
public class CustomerErrorConsumer {

    private CountDownLatch latch = new CountDownLatch(1);

    private Customer payload;

    @KafkaListener(topics = "wf-customer", groupId = "error-customer", errorHandler = "customerConsumerErrorHandler")
    public void consume(Customer customer) {
        if (customer.getId().equals("23")) {
            throw new RuntimeException("Invalid Message");
        }
        log.info("Message: {}", customer);
    }

    private void setPayload(Customer customer) {
        this.payload = customer;
    }

}
