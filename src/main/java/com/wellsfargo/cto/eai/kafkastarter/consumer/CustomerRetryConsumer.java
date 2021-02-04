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
public class CustomerRetryConsumer {

    private CountDownLatch latch = new CountDownLatch(1);

    private Customer payload;

    @KafkaListener(topics = "wf-customer", groupId = "retry-customer", containerFactory = "retryContainerFactory", errorHandler = "customerConsumerErrorHandler")
    public void consume(Customer customer) {
        log.info("Message: {}", customer);
    }

    private void setPayload(Customer customer) {
        this.payload = customer;
    }

}
