package tn.enit.tp4.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SensorDataEncoder implements Serializer<SensorData> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No special configuration required
    }

    @Override
    public byte[] serialize(String topic, SensorData data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize SensorData", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}