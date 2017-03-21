package kafkaProducer;

//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main {
	public static void main(String[] args) throws Exception{

		// Check arguments length value
		if(args.length == 0){
			System.out.println("test");
			return;
		}

		//Assign topicName to string variable
		String topicName = args[0].toString();

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"9.116.35.208:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	
		Producer<String, String> producer = new KafkaProducer
				<String, String>(props);
		
		try {
			String topic=topicName;
			String key = "mykey";
			String value = "myvalue";
			ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, key, value);
			producer.send(producerRecord);
		
			System.out.println("Message sent successfully");
			producer.close();
		}catch(Exception ex) { 
			ex.printStackTrace();
		}
	}

}
