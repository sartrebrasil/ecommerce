package ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CreateUserService {
	
	private final Connection connection;

	public CreateUserService() throws SQLException {
		String url = "jdbc:sqlite:target/users_databate.db";
		connection = DriverManager.getConnection(url);
		
		try {
			connection.createStatement().execute("create table users ("
					+ "uuid varchar(200) primary key, "
					+ "email varchar(200) )");			
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public static void main(String[] args) throws SQLException {
		CreateUserService createUserService = new CreateUserService();
		try (KafkaService<Order> kafkaService = new KafkaService<>(CreateUserService.class.getSimpleName(),
				"ECOMMERCE_NEW_ORDER", createUserService::parse, Order.class, new HashMap<String, String>());) {
			kafkaService.run();
		}
	}
	
	public void parse(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException, SQLException {
		System.out.println("----------------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		
		Order order = record.value();
		if (isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail());
		}
	}

	private void insertNewUser(String email) throws SQLException {
		PreparedStatement ps = connection.prepareStatement("insert into users (uuid, email) "
				+ "values (?,?)");
		ps.setString(1, UUID.randomUUID().toString());
		ps.setString(2, email);
		
		ps.execute();
		System.out.println("usuario uuid e " + email + " inserido");
	}

	private boolean isNewUser(String email) throws SQLException {
		PreparedStatement exists = connection.prepareStatement("select uuid from users where email = ? limit 1");
		exists.setString(1, email);
		ResultSet executeQuery = exists.executeQuery();
		return !executeQuery.next();
	}

}
