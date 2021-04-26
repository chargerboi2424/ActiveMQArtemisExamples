package com.chargerboi.ActiveMQArtemisExamples;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

public class fileMessageTopicListener implements MessageListener {

	@Override
	public void onMessage(Message message) {
		try {
			//Convert message to expected type
			BytesMessage messageReceived = (BytesMessage) message;
			
			//Get file information
			String fileName = (String) messageReceived.getObjectProperty("fileName");
			String extension = (String) messageReceived.getObjectProperty("fileExtension");
			
			//Debugging printout
			System.out.println("Filey McConsumer received " + extension + " file");

			//Get time for use in filename to prevent conflicts
			Instant instant = Instant.now();
			OffsetDateTime odt = instant.atOffset(ZoneOffset.UTC);
			String timeString = odt.format(DateTimeFormatter.ofPattern("uuuu-MM-dd-HH-mm-ss-SSS"));
			
			//Build filename and start write
			File outputFile = new File("test_files/" + timeString + "-" + fileName + "." + extension);
			FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
			BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutputStream);
			// This will block until the entire content is saved on disk
			//messageReceived.setObjectProperty("JMS_AMQ_SaveStream", bufferedOutput);
			// This won't wait the stream to finish. You need to keep the consumer active.
			messageReceived.setObjectProperty("JMS_AMQ_OutputStream", bufferedOutput);
			
			//Debugging printout
			System.out.println("Filey McConsumer finished receiving");
			
			//Calculate hash
			try {
				HashCode hc = Files.asByteSource(outputFile).hash(Hashing.sha256());
				System.out.println("SHA256 of file received is:" + hc.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (JMSException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
