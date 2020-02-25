package com.druid.kafka.kafkaspringdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class KafkaSpringDemoApplication {

	public static void main(String[] args) {
		//SpringApplication.run(KafkaSpringDemoApplication.class, args);
		ConfigurableApplicationContext context = SpringApplication.run(KafkaSpringDemoApplication.class, args);

		DruidKafkaProducer producer = context.getBean(DruidKafkaProducer.class);

		for (int i=0; i<3; i++){
			producer.send();
		}

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
