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
public class CustomerConsumer {

    private CountDownLatch latch = new CountDownLatch(1);

    private Customer payload;

    @KafkaListener(topics = "wf-customer", groupId = "wf-customer")
    public void consume(Customer customer) {
        setPayload(customer);
        log.info("Message: {}", customer);
    }

    private void setPayload(Customer customer) {
        this.payload = customer;
    }

}
