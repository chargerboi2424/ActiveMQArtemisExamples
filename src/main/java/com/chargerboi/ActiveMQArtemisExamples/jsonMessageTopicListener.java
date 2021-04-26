package com.chargerboi.ActiveMQArtemisExamples;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class jsonMessageTopicListener implements MessageListener {

	@Override
	public void onMessage(Message message) {
		try {
			//Debugging printout
			System.out.println("Recieving json message");
			
			//Convert message to expected type
			ObjectMessage jsonMessage = (ObjectMessage) message;
		    
			//Build json just to write to syso
			GsonBuilder builder = new GsonBuilder();
			System.out.println("Jsony McConsumer Received message: " + builder.create().toJson(jsonMessage.getObject()));
		    
			//Ideally you'd do something useful with the json, we're just going to dump it in a file here
			//FYI file doesn't preserve whitespace at all
		    try (Writer writer = new FileWriter("test_files/output.json")) {
				Gson gson = new GsonBuilder().create();
			    gson.toJson(jsonMessage.getObject(), writer);
		    }
		    
			//Debugging printout
		    System.out.println("Wrote out JSON file");
		} catch (JMSException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
