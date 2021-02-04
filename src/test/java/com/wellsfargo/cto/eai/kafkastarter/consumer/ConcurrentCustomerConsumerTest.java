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
public class ConcurrentCustomerConsumerTest {

    @Autowired
    private CustomerProducer customerProducer;

    @Autowired
    private ConcurrentCustomerConsumer concurrentCustomerConsumer;

    @Test
    public void testMultipleConsumers() throws InterruptedException {

        //given
        Customer customer1 = Customer.builder()
                .id("1")
                .firstName("alex")
                .lastName("smith")
                .phoneNumber("424645290").build();

        Customer customer2 = Customer.builder()
                .id("2")
                .firstName("ron")
                .lastName("stewart")
                .phoneNumber("424645290").build();
        customerProducer.sendData(customer1);
        customerProducer.sendData(customer2);
        concurrentCustomerConsumer.getLatch().await(10000, TimeUnit.MILLISECONDS);


        //then
        Assertions.assertThat(concurrentCustomerConsumer.getPartitions().size()).isEqualTo(2);
        Assertions.assertThat(concurrentCustomerConsumer.getPartitions()).contains(0, 1);
    }

}
