package ecommerce;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

	public static void main(String[] args) {
		EmailService emailService = new EmailService();
		try (KafkaService<Email> kafkaService = new KafkaService<>(
				EmailService.class.getSimpleName(), 
				"ECOMMERCE_SEND_EMAIL",
				emailService::parse, 
				Email.class, 
				new HashMap<String, String>());) {
			kafkaService.run();
		}
	}

	public void parse(ConsumerRecord<?, ?> record) {
		System.out.println("----------------------------------------------------");
		System.out.println("Sending email");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		System.out.println("Order processed");

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
