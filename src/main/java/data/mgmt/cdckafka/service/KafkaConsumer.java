package data.mgmt.cdckafka.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class KafkaConsumer {

    // properties에서 설정한 topic과 groupId
    @KafkaListener(topics = "kafka-demo", groupId = "kafka-demo")
    public void consume(String message) throws IOException {
        System.out.println(String.format("Consumed message: %s", message));
    }
}
