package ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

	@Override
	public void destroy() {
		orderDispatcher.close();
		emailDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//
//		try (KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>()) {
//			try (KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>()) {

		try {

			String emailAddress = req.getParameter("email");
			BigDecimal amount = new BigDecimal(req.getParameter("amount"));
			String orderId = UUID.randomUUID().toString();

			Order order = new Order(orderId, amount, emailAddress);
			orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAddress, order);

			Email email = new Email("sartre@gmail.com", "Thank you for order!! We are processng your order!");
			emailDispatcher.send("ECOMMERCE_SEND_EMAIL", emailAddress, email);

			System.out.println("New order sent successfully");

			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("New order sent successfully");

		} catch (InterruptedException | ExecutionException e) {
			throw new ServletException(e.getMessage(), e);
		}
//			}
//		}

	}

}
