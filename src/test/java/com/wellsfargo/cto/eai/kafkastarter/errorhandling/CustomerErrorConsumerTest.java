package com.wellsfargo.cto.eai.kafkastarter.errorhandling;

import com.wellsfargo.cto.eai.kafkastarter.KafkaStarterApplication;
import com.wellsfargo.cto.eai.kafkastarter.consumer.CustomerErrorConsumer;
import com.wellsfargo.cto.eai.kafkastarter.model.Customer;
import com.wellsfargo.cto.eai.kafkastarter.producer.CustomerProducer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = KafkaStarterApplication.class)
public class CustomerErrorConsumerTest {
    @Autowired
    private CustomerProducer customerProducer;

    @Autowired
    private CustomerErrorConsumer customerErrorConsumer;

    @Test
    public void testKafkaProducerAndConsumer() throws InterruptedException {

        //given
        Customer customer = Customer.builder()
                .id("23")
                .firstName("alex")
                .lastName("smith")
                .phoneNumber("424645290").build();
        customerProducer.sendData(customer);
        customerErrorConsumer.getLatch().await(10000, TimeUnit.MILLISECONDS);


        //then
        Assertions.assertThat(customerErrorConsumer.getPayload()).isNull();
    }

}
