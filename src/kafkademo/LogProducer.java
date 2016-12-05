package kafkademo;

import java.io.File;
import org.json.JSONObject;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.Json;

public class LogProducer {

	private Producer<String, String> inner;
	public LogProducer() throws Exception{
		Properties properties = new Properties();
		//File file = new File("/home/hadoop/Documents/data/producer.properties");
		File file = new File("producer.properties");          //root directory of this project
		FileInputStream fis = new FileInputStream(file);
		properties.load(fis);
		
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("metadata.broker.list", "localhost:9092");
		ProducerConfig config = new ProducerConfig(properties);
		inner = new Producer<String, String>(config);
	}
	
	public void send(String topicName, String message){
		if(topicName == null || message == null){
			return;
		}
		
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, message);
		inner.send(km);
	}
	
	public void send(String topicName, ArrayList<String> messages){
		if(topicName == null || messages == null){
            return;
        }
        if(messages.isEmpty()){
            return;
        }
        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
        for(String entry : messages){       		
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName,entry);
            kms.add(km);
        }
        inner.send(kms);
	}
	
	public void close(){
		inner.close();
	}
	
	public void testProducer(String msg){
		LogProducer producer = null;
		try{
			producer = new LogProducer();
			String messageStr1 = null;
			String messageStr2 = null;
			ArrayList<String> messages = new ArrayList<String>();
			JSONObject json = new JSONObject();
			
				messageStr1 = "this is a sample1_" + msg;
				messageStr2= "this is a sample2_" + msg;
				json.put("param1", messageStr1);
				json.put("param2", messageStr2);
				System.out.println("send:" + json.get("param1"));
				System.out.println("send:" + json.get("param2"));
				messages.add(json.toString());
						
			producer.send("test-topic", messages);									
		}catch(Exception e){
			e.printStackTrace();
		}
		finally{
			if(producer != null){
				producer.close();
			}
		}
	}
	
	public static void main(String[] args){
		LogProducer producer = null;
		try{
			producer = new LogProducer();
			String messageStr1 = null;
			String messageStr2 = null;
			ArrayList<String> messages = new ArrayList<String>();
			JSONObject json = new JSONObject();
			for(int i=0; i<10; i++){
				messageStr1 = "this is a sample1_" + i;
				messageStr2= "this is a sample2_" + i;
				json.put("param1", messageStr1);
				json.put("param2", messageStr2);
				System.out.println("send:" + json.get("param1"));
				System.out.println("send:" + json.get("param2"));
				messages.add(json.toString());
			}			
			producer.send("test-topic", messages);									
		}catch(Exception e){
			e.printStackTrace();
		}
		finally{
			if(producer != null){
				producer.close();
			}
		}
	}
}
