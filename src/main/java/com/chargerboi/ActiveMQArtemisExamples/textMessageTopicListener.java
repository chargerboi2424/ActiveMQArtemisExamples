package com.chargerboi.ActiveMQArtemisExamples;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

public class textMessageTopicListener implements MessageListener {

	@Override
	public void onMessage(Message message) {
		try {
			//Convert message to expected type
			TextMessage textMessage = (TextMessage) message;
			
			//Debugging printout, ideally you'd do something with the message
			System.out.println("Texty McConsumer received message: " + textMessage.getText());
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
