package com.chargerboi.ActiveMQArtemisExamples;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;

public class Consumer implements MessageListener{
	
	InitialContext initialContext = null;
	Connection connection = null;
	ConnectionFactory cf = null;
	Session session = null;
	
	public static void main(final String[] args) throws Exception {
		System.out.println("Starting consumers");
		Consumer consumerObj = new Consumer();
		consumerObj.createConsumers();
		System.out.println("Shutting down");
	}
	
	public void createConsumers() throws Exception {
		try {
			
			//Setup AMQ
			initialContext = new InitialContext();
			cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
			connection = cf.createConnection("admin","admin");
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			//Subscribe to the text message topic
			Topic textMessageTopic = (Topic) initialContext.lookup("topic/textMessageTopic");
			MessageConsumer textMessageTopicConsumer = session.createConsumer(textMessageTopic);
			textMessageTopicConsumer.setMessageListener(new textMessageTopicListener());
			
			//Subscribe to the file message topic
			Topic fileMessageTopic = (Topic) initialContext.lookup("topic/fileMessageTopic");
			MessageConsumer fileMessageTopicConsumer = session.createConsumer(fileMessageTopic);
			fileMessageTopicConsumer.setMessageListener(new fileMessageTopicListener());
			
			//Subscribe to the json message topic
			Topic jsonMessageTopic = (Topic) initialContext.lookup("topic/jsonMessageTopic");
			MessageConsumer jsonMessageTopicConsumer = session.createConsumer(jsonMessageTopic);
			jsonMessageTopicConsumer.setMessageListener(new jsonMessageTopicListener());
			
			//Init producer to be able to reply to get rest call messages
			Topic getRestCallResponseTopic = (Topic) initialContext.lookup("topic/getRestCallResponseTopic");
			MessageProducer getRestCallResponseTopicProducer = session.createProducer(getRestCallResponseTopic);
			
			//Subscribe to the get rest call message topic
			Topic getRestCallRequestTopic = (Topic) initialContext.lookup("topic/getRestCallRequestTopic");
			MessageConsumer getRestCallRequestTopicConsumer = session.createConsumer(getRestCallRequestTopic);
			getRestCallRequestTopicConsumer.setMessageListener(new getRestCallRequestTopicListener(session, getRestCallResponseTopicProducer));
			
			//Init producer to be able to reply to post rest call messages
			Topic postRestCallResponseTopic = (Topic) initialContext.lookup("topic/postRestCallResponseTopic");
			MessageProducer postRestCallResponseTopicProducer = session.createProducer(postRestCallResponseTopic);
			
			//Subscribe to the post rest call message topic
			Topic postRestCallRequestTopic = (Topic) initialContext.lookup("topic/postRestCallRequestTopic");
			MessageConsumer postRestCallRequestTopicConsumer = session.createConsumer(postRestCallRequestTopic);
			postRestCallRequestTopicConsumer.setMessageListener(new postRestCallRequestTopicListener(session, postRestCallResponseTopicProducer));

			//Subscribe to the shutdown request topic
			Topic shutdownTopic = (Topic) initialContext.lookup("topic/shutdownTopic");
			MessageConsumer shutdownTopicConsumer1 = session.createConsumer(shutdownTopic);
			shutdownTopicConsumer1.setMessageListener(this);

			//Sync so we can shutdown when getting a shutdown request
			synchronized (this) {
				connection.start();
				this.wait();
			}
			
			//Shutdown
			connection.stop();
			session.close();
		} finally {
			//Cleanup if an error occurred
			if (connection != null) {
				connection.close();
			}

			if (initialContext != null) {
				initialContext.close();
			}
		}
	}

	@Override
	public void onMessage(Message message) {
		//We received a shutdown request, sync so the main thread can die
		System.out.println("Shutdown request received");
		synchronized (this) {
			this.notifyAll();
		}
	}
	
}
