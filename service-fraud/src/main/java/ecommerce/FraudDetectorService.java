package ecommerce;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

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
	
	private KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	public void parse(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException {
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
		
		Order order = record.value();
		if (isFraud(order)) {
			System.out.println("Order is a fraud!!! " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
			
		} else {
			System.out.println("Order is a not fraud!!! " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
		}
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}

}
