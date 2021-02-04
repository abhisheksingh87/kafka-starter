package com.wellsfargo.cto.eai.kafkastarter.consumer;

import com.wellsfargo.cto.eai.kafkastarter.model.Customer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Service
@Slf4j
@Getter
public class ConcurrentCustomerConsumer {

    private CountDownLatch latch = new CountDownLatch(1);

    private List<Integer> partitions = new ArrayList<>();

    @KafkaListener(topics = "wf-customer", groupId = "wf-customer", concurrency = "3")
    public void consume(ConsumerRecord<String, Customer> consumerRecord) {
        setPartitions(consumerRecord.partition());
        log.info("Partition: {}, Offset: {}, Message: {}", consumerRecord.partition(),
                consumerRecord.offset(), consumerRecord.value());
    }

    private void setPartitions(int partition) {
        this.partitions.add(partition);
    }

}
