package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.Customer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;


@Slf4j
@Getter
public class CustomerConsumer {

    private Customer customer;

    @KafkaListener(topics = "wf-customer", groupId = "wf-customer")
    public void consume(Customer customer) {
        setCustomer(customer);
        log.info("Message: {}", customer);
    }

    /**
     * There is no business logic as such in the
     * consume method we are using setCustomer just for testing purpose
     * @param customer
     */
    private void setCustomer(Customer customer) {
        this.customer = customer;
    }

}
