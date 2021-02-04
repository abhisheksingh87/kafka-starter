package com.wellsfargo.cto.eai.kafkastarter.producer;

import com.wellsfargo.cto.eai.kafkastarter.KafkaStarterApplication;
import com.wellsfargo.cto.eai.kafkastarter.model.Customer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = KafkaStarterApplication.class)
public class KafkaProducerTest {

    @Autowired
    private CustomerProducer customerProducer;


    @Test
    public void testCustomerProducer() {

        //given
        Customer customer = Customer.builder()
                                    .id("23")
                                    .firstName("alex")
                                    .lastName("smith")
                                    .phoneNumber("424645290").build();

        //when
        customerProducer.sendData(customer);

        //then

    }
}
