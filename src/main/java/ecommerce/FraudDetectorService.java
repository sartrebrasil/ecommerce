package ecommerce;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) {
		FraudDetectorService fraudDetectorService = new FraudDetectorService();
		try (KafkaService<Order> kafkaService = new KafkaService<>(
				FraudDetectorService.class.getSimpleName(), 
				"ECOMMERCE_NEW_ORDER",
				fraudDetectorService::parse, 
				Order.class,
				new HashMap<String, String>());) {
			kafkaService.run();
		}
	}

	public void parse(ConsumerRecord<String, Order> record) {
		System.out.println("----------------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		System.out.println("Order processed");

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
