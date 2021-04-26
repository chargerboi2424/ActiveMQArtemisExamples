package com.chargerboi.ActiveMQArtemisExamples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.google.gson.GsonBuilder;

public class getRestCallRequestTopicListener implements MessageListener {

	Session session = null;
	MessageProducer responseMessageProducer = null;
	
	public getRestCallRequestTopicListener(Session sessionIn, MessageProducer responseMessageProducerIn) {
		session = sessionIn;
		responseMessageProducer = responseMessageProducerIn;
	}

	@Override
	public void onMessage(Message message) {
		try {
			//Convert message to expected type
			TextMessage textMessage = (TextMessage) message;
			
			//Get URL for rest get call
			String url_string = textMessage.getText();
			
			//Debugging printout
			System.out.println("Resty McConsumer received url: " + url_string);
			
			//Make rest get call
			URL url = new URL( url_string);
			HttpURLConnection http = (HttpURLConnection)url.openConnection();
			http.setRequestProperty("Accept", "application/json");

			//Read in all of the response
			BufferedReader rd = new BufferedReader(new InputStreamReader(http.getInputStream(), "UTF-8"));
			String responseBody = rd.lines().collect(Collectors.joining("\n"));
			http.disconnect();
			
			//Debugging printout
			System.out.println(http.getResponseCode() + " " + http.getResponseMessage());
			System.out.println(responseBody);
			
			//Build json object to send back from response
		    GsonBuilder builder = new GsonBuilder();
		    Object responseObject = builder.create().fromJson(responseBody, Object.class);
		    
		    //Send back response on different topic
		    ObjectMessage returnMessage = session.createObjectMessage((Serializable) responseObject);
		    returnMessage.setObjectProperty("responseCode", http.getResponseCode());
		    returnMessage.setObjectProperty("responseMessage", http.getResponseMessage());
		    returnMessage.setObjectProperty("UUID", textMessage.getObjectProperty("UUID"));
		    responseMessageProducer.send(returnMessage);
		} catch (JMSException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
