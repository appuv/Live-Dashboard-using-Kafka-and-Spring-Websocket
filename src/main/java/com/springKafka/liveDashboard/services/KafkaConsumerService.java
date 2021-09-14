package com.springKafka.liveDashboard.services;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

	@Autowired
	SimpMessagingTemplate template;

	@KafkaListener(topics = "${kafka.topic}")
	public void consume(@Payload String message) {
		try {
			JSONObject jsonObject = new JSONObject(message);
			System.out.println("Input data is "+message);
			System.out.println("Serial is "+jsonObject.get("serial"));
			boolean found = false;
			int temp = 0;
			if (jsonObject.get("serial").toString().equalsIgnoreCase("1")) {
				found = true;
				temp = Integer.parseInt(jsonObject.get("temp").toString());
				System.out.println("Found data for Id 1, temp live is "+temp);

			}
			if (found) {
				System.out.println("sending temp "+temp);

				template.convertAndSend("/topic/temperature", temp);
			}

		} catch (Exception e) {
			System.out.println("error in parsing message " + message);
			e.printStackTrace();
		}

	}
}
