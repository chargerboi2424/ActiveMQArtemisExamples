package com.chargerboi.ActiveMQArtemisExamples;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Scanner;
import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.gson.GsonBuilder;

public class TestClient {

	//System in for reading input
	static Scanner scanner = null;
	
	//AMQ references
	static InitialContext initialContext = null;
	static ConnectionFactory cf = null;
	static Connection connection = null;
	static Session session = null;
	
	//AMQ producer references
	static MessageProducer textMessageTopicProducer = null;
	static MessageProducer fileMessageTopicProducer = null;
	static MessageProducer jsonMessageTopicProducer = null;
	static MessageProducer getRestCallRequestTopicProducer = null;
	static MessageProducer postRestCallRequestTopicProducer = null;
	static MessageProducer shutdownTopicProducer = null;
	
	//AMQ consumer references
	static MessageConsumer getRestCallResponseTopicConsumer = null;
	static MessageConsumer postRestCallResponseTopicConsumer = null;
	
	public static void main(final String[] args) throws Exception {
		try {
			//Initialize AMQ connection
			initialContext = new InitialContext();
			cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
			connection = cf.createConnection("admin","admin");
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			//Initialize all the producers
			Topic textMessageTopic = (Topic) initialContext.lookup("topic/textMessageTopic");
			textMessageTopicProducer = session.createProducer(textMessageTopic);

			Topic fileMessageTopic = (Topic) initialContext.lookup("topic/fileMessageTopic");
			fileMessageTopicProducer = session.createProducer(fileMessageTopic);

			Topic jsonMessageTopic = (Topic) initialContext.lookup("topic/jsonMessageTopic");
			jsonMessageTopicProducer = session.createProducer(jsonMessageTopic);
			
			Topic getRestCallRequestTopic = (Topic) initialContext.lookup("topic/getRestCallRequestTopic");
			getRestCallRequestTopicProducer = session.createProducer(getRestCallRequestTopic);
			
			Topic postRestCallRequestTopic = (Topic) initialContext.lookup("topic/postRestCallRequestTopic");
			postRestCallRequestTopicProducer = session.createProducer(postRestCallRequestTopic);
			
			Topic shutdownTopic = (Topic) initialContext.lookup("topic/shutdownTopic");
			shutdownTopicProducer = session.createProducer(shutdownTopic);
			
			//Setup consumers for rest call returns
			Topic getRestCallResponseTopic = (Topic) initialContext.lookup("topic/getRestCallResponseTopic");
			getRestCallResponseTopicConsumer = session.createConsumer(getRestCallResponseTopic);
			
			Topic postRestCallResponseTopic = (Topic) initialContext.lookup("topic/postRestCallResponseTopic");
			postRestCallResponseTopicConsumer = session.createConsumer(postRestCallResponseTopic);
			
			//Connect
			connection.start();

			//Enter loop for user input
			int option = 0;

			while (option != 7)
			{
				option = showMenu();
				switch (option) {
				case 1:
					sendTextMessage();
					break;
				case 2:
					sendFileMessage();
					break;
				case 3:
					sendJSONObject();
					break;
				case 4:
					sendGetRestCallRequest();
					break;
				case 5:
					sendPostRestCallRequest();
					break;
				case 6:
					sendShutdownMessage();
					break;
				case 7:
					break;
				default:
					System.out.println("Sorry, please enter valid option");
				}
			}
		} finally {
			//Cleanup
			if(scanner != null) {
				scanner.close();
			}
			if (connection != null) {
				connection.close();
			}

			if (initialContext != null) {
				initialContext.close();
			}
		}
	}

	private static int showMenu() {
		//Init scanner for input, since this is ran first it will always be ready for the other methods
		if(scanner == null) {
			scanner = new Scanner(System.in);
		}
		
		//Write menu out
	    System.out.println("------------------------------------------------------------------------------------");
	    System.out.println("1. Send text message on textMessageTopic");
	    System.out.println("2. Send file message on fileMessageTopic");
	    System.out.println("3. Send json message on jsonMessageTopic");
	    System.out.println("4. Request GET rest call on restCallRequestTopic");
	    System.out.println("5. Request POST rest call on restCallRequestTopic");
	    System.out.println("6. Send message on shutdownTopic");
	    System.out.println("7. Exit");
	    System.out.println("------------------------------------------------------------------------------------");
	    System.out.println("Select an option:");
	    
	    //Read full line, but parse for int
	    //TODO add error handling, it's easy to break but its a test client so...
	    return Integer.parseInt(scanner.nextLine());
	}
	
	private static void sendTextMessage() throws JMSException {
		//Read in text 
		System.out.println("Enter contents for message:");
		String contents = scanner.nextLine();
		
		//Make message
		TextMessage textMessage = session.createTextMessage(contents);
		textMessageTopicProducer.send(textMessage);
		
		//Debugging printout
		System.out.println("Sent message on textMessageTopic: " + textMessage.getText());
	}
	
	private static void sendFileMessage() throws JMSException, FileNotFoundException {
		//Read in filename to send, relative to root project folder
		System.out.println("Enter filename to send in message (default: test_files/send.jpg):");
		String filepath = scanner.nextLine();
		
		//If blank, use default
		if(filepath.isBlank()) {
			filepath = "test_files/send.jpg";
		}
		
		//Create file object for path entered
		//TODO add error handling
		File file = new File(filepath);
		
		//Debugging printout
		System.out.println(file.getAbsolutePath());
		
		//Get filename and extension to embed in message as well
		String filename = file.getName();
		String extension = "";
		
		int lastPeriod = filename.lastIndexOf('.');
		if (lastPeriod > 0) {
		    extension = filename.substring(lastPeriod+1);
		    filename = filename.substring(0, lastPeriod);
		}
		else {
			System.out.println("Extenionless files not supported yet");
			return;
		}
		
		//Build message
		BytesMessage fileMessage = session.createBytesMessage();		
		FileInputStream fileInputStream = new FileInputStream(file);
		BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);
		fileMessage.setObjectProperty("JMS_AMQ_InputStream", bufferedInput);
		fileMessage.setObjectProperty("fileName", filename);
		fileMessage.setObjectProperty("fileExtension", extension);
		
		//Send message
		fileMessageTopicProducer.send(fileMessage);
		
		//Debugging printout
		System.out.println("Sent file message on fileMessageTopic");
		
		//Calculate hash and compare
		//Store the hash sent
		String sentHash = "";
		String copiedHash = "";
		
		//Calculate
		try {
			HashCode hc = Files.asByteSource(file).hash(Hashing.sha256());
			sentHash = hc.toString();
			System.out.println("SHA256 of file sent is:" + sentHash);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//I got tired of comparing by hand to make sure it worked, so we're comparing them automatically
		//Get copied hash from consumer program's output
		System.out.println("Enter hash from consumer's output:");
		copiedHash = scanner.nextLine();
		
		//Compare hashes
		if(sentHash.equals(copiedHash.strip())) {
			System.out.println("PASS - Hashes match");
		}
		else {
			System.out.println("FAIL - Hashes do not match");
		}
		
	}
	
	private static void sendJSONObject() throws FileNotFoundException, JMSException {
		//Get filename to read json out of, this test client just supports files
		System.out.println("Enter filename to send in message (default: test_files/test.json):");
		String filename = scanner.nextLine();
		
		//If blank, use default
		if(filename.isBlank()) {
			filename = "test_files/test.json";
		}
		
		//Create file object for path entered
		//TODO add error handling
		File file = new File(filename);
		
		//Debugging printout
		System.out.println(file.getAbsolutePath());
		
		//Read in file and create json object to send
	    BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
	    GsonBuilder builder = new GsonBuilder();
	    Object o = builder.create().fromJson(bufferedReader, Object.class);

	    //Build message
	    ObjectMessage jsonMessage = session.createObjectMessage((Serializable) o);
	    
	    //Send message
	    jsonMessageTopicProducer.send(jsonMessage);
	    
	    //Debugging printout
	    System.out.println("Sent json message");
	}
	
	private static void sendGetRestCallRequest() throws JMSException {
		//Read in URL to run the get rest call against
		System.out.println("Enter URL to send in message (default: https://reqbin.com/echo/get/json):");
		String url = scanner.nextLine();
		
		//If blank, use default
		if(url.isBlank()) {
			url = "https://reqbin.com/echo/get/json";
		}
		
		//Create UUID to identify request, the response will have the same one on it
		String uuid = UUID.randomUUID().toString();
		
		//Build message to send
		TextMessage textMessage = session.createTextMessage(url);
		textMessage.setObjectProperty("UUID", uuid);
		
		//Send message
		getRestCallRequestTopicProducer.send(textMessage);
		
		//Debugging printout
		System.out.println("Sent GET request " + uuid);
		
		//Wait for return message and print message
		ObjectMessage messageReceived = (ObjectMessage) getRestCallResponseTopicConsumer.receive();
		GsonBuilder builder = new GsonBuilder();
		System.out.println("Recieved back status for " + messageReceived.getObjectProperty("UUID") + ": " + messageReceived.getObjectProperty("responseCode") + " - " + messageReceived.getObjectProperty("responseMessage"));
		System.out.println("Recieved back response for "  + messageReceived.getObjectProperty("UUID") + ": "  + builder.create().toJson(messageReceived.getObject()));
	}
	
	private static void sendPostRestCallRequest() throws JMSException {
		//Read in URL to run the post rest call against
		System.out.println("Enter URL to send in message (default: https://reqbin.com/echo/post/json):");
		String url = scanner.nextLine();
		
		//If blank, use default
		if(url.isBlank()) {
			url = "https://reqbin.com/echo/post/json";
		}
		
		//Read in the json to send in the body
		System.out.println("Enter json to send in message (default: {\\\"Id\\\":78912.0,\\\"Customer\\\":\\\"Jason Sweet\\\",\\\"Quantity\\\":1.0,\\\"Price\\\":18.0}):");
		String json = scanner.nextLine();
		
		//If blank, use default
		if(json.isBlank()) {
			json = "{\"Id\":78912.0,\"Customer\":\"Jason Sweet\",\"Quantity\":1.0,\"Price\":18.0}";
		}
		
		//Create json object to send in message from json string
	    GsonBuilder builder = new GsonBuilder();
	    Object o = builder.create().fromJson(json, Object.class);
	    
	    //Create UUID to identify request, the response will have the same one on it
	    String uuid = UUID.randomUUID().toString();
	    
	    //Build message to send
	    ObjectMessage jsonMessage = session.createObjectMessage((Serializable) o);
	    jsonMessage.setObjectProperty("URL", url);
		jsonMessage.setObjectProperty("UUID", uuid);
		
		//Send message
		postRestCallRequestTopicProducer.send(jsonMessage);
		
		//Debugging printout
		System.out.println("Sent Post request " + uuid);
		
		//Wait for return message and print message
		ObjectMessage messageReceived = (ObjectMessage) postRestCallResponseTopicConsumer.receive();
		System.out.println("Recieved back status for " + messageReceived.getObjectProperty("UUID") + ": " + messageReceived.getObjectProperty("responseCode") + " - " + messageReceived.getObjectProperty("responseMessage"));
		System.out.println("Recieved back response for "  + messageReceived.getObjectProperty("UUID") + ": "  + builder.create().toJson(messageReceived.getObject()));
	}
	
	private static void sendShutdownMessage() throws JMSException {
		//This sends a message on a special topic that causes the consumer app to shutdown since it's not interactive, could be used with docker for a restart if combined with a heartbeat
		TextMessage message3 = session.createTextMessage("The contents literally doesn't matter");
		shutdownTopicProducer.send(message3);
		
		//Debugging printout
		System.out.println("Sent message on shutdownTopic: " + message3.getText());
	}
}
