package kafkaProducer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
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

		String inFileName = "/home/yakumo/Datasets/wiki";
		BufferedReader in = null;
		try {
			InputStream rawin = new FileInputStream(inFileName);
			if (inFileName.endsWith(".gz"))
				rawin = new GZIPInputStream(rawin);
			in = new BufferedReader(new InputStreamReader(rawin));
		} catch (FileNotFoundException e) {
			System.err.println("File not found");
			e.printStackTrace();
			System.exit(1);
		}

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"9.116.35.208:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

		Producer<String, String> producer = new KafkaProducer
				<String, String>(props);

		try {
			String topic=topicName;
			

			String line = in.readLine();
			while(line != null) {
				String []tokens = line.split("\t");
				String key = tokens[0];
				String value = tokens[1];
				ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, key, value);
				producer.send(producerRecord);
				line = in.readLine();
			}
			System.out.println("Message sent successfully");
			producer.close();
		}catch(Exception ex) { 
			ex.printStackTrace();
		}
	}

}
