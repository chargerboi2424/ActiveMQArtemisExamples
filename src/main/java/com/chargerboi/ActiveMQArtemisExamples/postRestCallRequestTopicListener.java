package com.chargerboi.ActiveMQArtemisExamples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import com.google.gson.GsonBuilder;

public class postRestCallRequestTopicListener implements MessageListener {

	MessageProducer responseMessageProducer = null;
	Session session = null;
	
	public postRestCallRequestTopicListener(Session sessionIn, MessageProducer responseMessageProducerIn) {
		session = sessionIn;
		responseMessageProducer = responseMessageProducerIn;
	}
	
	@Override
	public void onMessage(Message message) {
		try {
			//Convert message to expected type
			ObjectMessage jsonMessage = (ObjectMessage) message;
		    
			//Create json string to send across in call
			GsonBuilder builder = new GsonBuilder();
			String jsonString = builder.create().toJson(jsonMessage.getObject());
			
			//Get URL for rest post call
			String urlString = (String) jsonMessage.getObjectProperty("URL");
			
			//Debugging printout
			System.out.println("Resty McConsumer Received message: " + urlString);
			System.out.println("Resty McConsumer Received message: " + jsonString);
			
			//Make rest post call
			URL url = new URL(urlString);
			HttpURLConnection http = (HttpURLConnection)url.openConnection();
			http.setRequestMethod("POST");
			http.setDoOutput(true);
			http.setRequestProperty("Accept", "application/json");
			http.setRequestProperty("Content-Type", "application/json");

			//Write post body
			byte[] out = jsonString.getBytes(StandardCharsets.UTF_8);
			OutputStream stream = http.getOutputStream();
			stream.write(out);
			
			//Read in all of the response
			BufferedReader rd = new BufferedReader(new InputStreamReader(http.getInputStream(), "UTF-8"));
			String responseBody = rd.lines().collect(Collectors.joining("\n"));
			
			//Debugging printout
			System.out.println(http.getResponseCode() + " " + http.getResponseMessage());
			System.out.println(responseBody);

			//Disconnect
			http.disconnect();

			//Build json object to send back from response
			Object responseObject = builder.create().fromJson(responseBody, Object.class);
		    
		    //Send back response on different topic
		    ObjectMessage returnMessage = session.createObjectMessage((Serializable) responseObject);
		    returnMessage.setObjectProperty("responseCode", http.getResponseCode());
		    returnMessage.setObjectProperty("responseMessage", http.getResponseMessage());
		    returnMessage.setObjectProperty("UUID", jsonMessage.getObjectProperty("UUID"));
		    responseMessageProducer.send(returnMessage);
			
		} catch (JMSException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
