import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Application {
	private static final String TOPIC = "events";
	// private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	public static void main(String [] args) {
		String consumerGroup = "defaultConsumerGroup";
		if(args.length==1) {
			consumerGroup = args[0];
		}
		System.out.println("Consumer is part of consumer group "+ consumerGroup);
		
		Consumer<Long, String> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
		
		consumeMessages(TOPIC, kafkaConsumer);
	}
	
	public static void consumeMessages(String topic, Consumer<Long, String> kafkaConsumer) {
		kafkaConsumer.subscribe(Collections.singletonList(topic));
		while(true) {
			// Kafka may batch multiple records for network efficiency
			ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
			
			if (consumerRecords.isEmpty()) {
				// do something else
			}
			for (ConsumerRecord<Long, String> record : consumerRecords) {
				System.out.println(String.format("Received Record (key: %d, value: %s, partition: %d, offset: %d", 
						record.key(),record.value(), record.partition(), record.offset()));
			}
			
			// do something with the records
			
			kafkaConsumer.commitAsync();
			
		}
	}
	
	public static Consumer<Long, String> createKafkaConsumer(String bootstrapServers, String consumerGroup){
		Properties properties = new Properties();
		
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		// Everytime a consumer from a consumer group reads a message from a topic,
		// Kafka needs to be notified that a message has already been consumed to make sure consumer will not be sent same message twice
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		
		// read twice in case consumer crashed
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		
		return new KafkaConsumer<Long, String>(properties);
	}
}
