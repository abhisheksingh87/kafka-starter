package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.Customer;
import com.wellsfargo.cto.eai.kafkastarter.producer.CustomerProducer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class CustomerConsumerTest {

    @Autowired
    private CustomerProducer customerProducer;

    @Autowired
    private CustomerConsumer customerConsumer;

    @Test
    public void testKafkaProducerAndConsumer() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        //given
        Customer customer = Customer.newBuilder()
                .setId("1")
                .setFirstName("alex")
                .setLastName("smith")
                .setPhoneNumber("424645290").build();
        customerProducer.sendSinglePartitionTopic(customer);
        countDownLatch.await(10000, TimeUnit.MILLISECONDS);


        //then
        Assertions.assertThat(customerConsumer.getCustomer()).usingRecursiveComparison().isEqualTo(customer);
    }

    @Test
    public void testMultipleConsumers() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);
        //given
        Customer customer = Customer.newBuilder()
                .setId("1")
                .setFirstName("alex")
                .setLastName("smith")
                .setPhoneNumber("424645290").build();
        customerProducer.sendSinglePartitionTopic(customer);
        countDownLatch.await(10000, TimeUnit.MILLISECONDS);


        //then
        Assertions.assertThat(customerConsumer.getCustomer()).usingRecursiveComparison().isEqualTo(customer);
    }
}
