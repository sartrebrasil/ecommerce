package ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		try (KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>()) {
			try (KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>()) {
				
				String emailAddress = Math.random() + "@email.com";
				for (int i = 0; i < 10; i++) {

					String orderId = UUID.randomUUID().toString();
					BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
					
					Order order = new Order(orderId, amount, emailAddress);
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAddress, order);
	
					Email email = new Email("sartre@gmail.com", "Thank you for order!! We are processng your order!");
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", emailAddress, email);
				}
			}
		}

	}

}
