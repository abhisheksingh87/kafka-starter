package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.Customer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Getter
@Service
public class CustomerRetryConsumer {

    @KafkaListener(topics = "wf-retry-customer", groupId = "retry-customer", containerFactory = "retryContainerFactory", errorHandler = "customerConsumerErrorHandler")
    public void consume(Customer customer) {
        log.info("Message: {}", customer);
    }

}
