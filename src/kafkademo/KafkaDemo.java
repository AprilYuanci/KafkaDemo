package kafkademo;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import kafkademo.LogConsumer.MessageExecutor;

public class KafkaDemo {

	public static void main(String[] args){
		KafkaProducer producerThread = new KafkaProducer(KafkaProperties.topic);
		producerThread.start();
		
		KafkaConsumer consumerThread = null;
		try{
            MessageExecutor executor = new MessageExecutor() {               
                public void execute(String message) {
                		try {
								JSONObject jsonStr = new JSONObject(message);
								System.out.println("received:" + jsonStr.get("param1") + " & " + jsonStr.get("param2"));				
							} catch (JSONException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}}};
				consumerThread = new KafkaConsumer(KafkaProperties.topic,2,executor);
				consumerThread.start();
	}catch(Exception e){
        e.printStackTrace();
    }finally{
        if(consumerThread != null){
        	consumerThread.stop();
        }
    }
}
	@Test
	public void testKafka(){
		LogProducer producer;
		
		try {
			producer = new LogProducer();
			String dataid = "hello";
			producer.testProducer(dataid);
			
			LogConsumer.testConsumer();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
