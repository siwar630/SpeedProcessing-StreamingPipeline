package tn.enit.tp4.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class SensorDataProducer {

    private final KafkaProducer<String, SensorData> producer;

    public SensorDataProducer(KafkaProducer<String, SensorData> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {
        // Configure the producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "tn.enit.tp4.kafka.SensorDataEncoder");

        // Create a Kafka producer
        KafkaProducer<String, SensorData> producer = new KafkaProducer<>(properties);

        // Instantiate the SensorDataProducer and generate events
        SensorDataProducer sensorProducer = new SensorDataProducer(producer);
        sensorProducer.generateIoTEvent("sensor-data-topic");
    }

    private void generateIoTEvent(String topic) throws InterruptedException {
        Random rand = new Random();
        double initTemp = 20;
        double initHum = 80;
        System.out.println("Sending events...");

        while (true) {
            SensorData event = generateSensorData(rand, initTemp, initHum);
            producer.send(new ProducerRecord<>(topic, event.getId(), event));
            Thread.sleep(rand.nextInt(3000) + 2000); // Random delay between 2 and 5 seconds
        }
    }

    private SensorData generateSensorData(final Random rand, double temp, double hum) {
        String id = UUID.randomUUID().toString();
        Date timestamp = new Date();
        double temperature = temp + rand.nextDouble() * 10;
        double humidity = hum + rand.nextDouble() * 10;

        return new SensorData(id, temperature, humidity, timestamp);
    }
}