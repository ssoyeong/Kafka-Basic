package data.mgmt.cdckafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    // properties에서 설정한 topic
    private static final String TOPIC = "kafka-demo";
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        System.out.println(String.format("Produce message: %s", message));
        // topic에 해당하는 message 전달
        this.kafkaTemplate.send(TOPIC, message);
    }
}
