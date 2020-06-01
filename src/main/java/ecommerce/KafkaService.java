package ecommerce;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable {
	
	private final KafkaConsumer<String, T> consumer;
	private final ConsumerFunction<T> function;
	
	KafkaService(String groupId, String topic, ConsumerFunction<T> function, Class<T> type, Map<String, String> params) {
		this(groupId, function, type, params);
		this.consumer.subscribe(Collections.singletonList(topic));
	}
	
	KafkaService(String groupId, Pattern topic, ConsumerFunction<T> function, Class<T> type, Map<String, String> params) {
		this(groupId, function, type, params);
		this.consumer.subscribe(topic);
	}
	
	private KafkaService(String groupId, ConsumerFunction<T> function, Class<T> type, Map<String, String> params) {
		this.function = function;
		this.consumer = new KafkaConsumer<>(getProperties(type, groupId, params));
		
	}
	
	void run() {
		while(true) {
			ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
			if (!records.isEmpty()) {
				System.out.println("encotrei " + records.count() + " registros");
				records.forEach(( record ) -> {
					function.consume(record);
				});
			}
		}
	}

	Properties getProperties(Class<T> type, String groupId, Map<String, String> overridesProperties) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.put(GsonDeserializer.TYPE_CONFIG, type.getName());
		properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.putAll(overridesProperties);
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}
	
}
