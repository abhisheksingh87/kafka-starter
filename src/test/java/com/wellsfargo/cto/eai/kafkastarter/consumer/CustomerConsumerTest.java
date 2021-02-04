package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.KafkaStarterApplication;
import com.wellsfargo.cto.eai.kafkastarter.model.Customer;
import com.wellsfargo.cto.eai.kafkastarter.producer.CustomerProducer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = KafkaStarterApplication.class)
public class CustomerConsumerTest {

    @Autowired
    private CustomerProducer customerProducer;

    @Autowired
    private CustomerConsumer customerConsumer;

    @Test
    public void testKafkaProducerAndConsumer() throws InterruptedException {

        //given
        Customer customer = Customer.builder()
                .firstName("alex")
                .lastName("smith")
                .phoneNumber("424645290").build();
        customerProducer.sendData(customer);
        customerConsumer.getLatch().await(10000, TimeUnit.MILLISECONDS);


        //then
        Assertions.assertThat(customerConsumer.getLatch().getCount()).isEqualTo(1L);
        Assertions.assertThat(customerConsumer.getPayload()).usingRecursiveComparison().isEqualTo(customer);
    }

    @Test
    public void testMultipleConsumers() throws InterruptedException {

        //given
        Customer customer = Customer.builder()
                .firstName("alex")
                .lastName("smith")
                .phoneNumber("424645290").build();
        customerProducer.sendData(customer);
        customerConsumer.getLatch().await(10000, TimeUnit.MILLISECONDS);


        //then
        Assertions.assertThat(customerConsumer.getLatch().getCount()).isEqualTo(1L);
        Assertions.assertThat(customerConsumer.getPayload()).usingRecursiveComparison().isEqualTo(customer);
    }
}
