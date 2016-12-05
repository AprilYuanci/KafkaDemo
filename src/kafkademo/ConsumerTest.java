package kafkademo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.consumer.Consumer;

public class ConsumerTest {

	private String groupid;
	
	private String consumerid;
	
	private int threadPerTopic;
	
	public ConsumerTest(String groupid, String consumerid, int threadPerTopic){
		super();
		this.groupid = groupid;
		this.consumerid = consumerid;
		this.threadPerTopic = threadPerTopic;
	}
	
	public void consume(){
		Properties props = new Properties();
		props.put("group.id", groupid);
		props.put("consumer.id", consumerid);
		props.put("zookeeper.connect", KafkaProperties.zkConnect);
		props.put("zookeeper.session.timeout.ms", "60000");
		props.put("zookeeper.sync.time.ms", "2000");
		
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(KafkaProperties.topic, threadPerTopic);
		
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);
		
		for(KafkaStream<byte[], byte[]> stream: streams.get(KafkaProperties.topic)){
			//new MyStreamThread(stream).start();
			executeMsg(stream);
			
			/*ConsumerIterator<byte[], byte[]> streamIterator = stream.iterator();
			while(streamIterator.hasNext()) {
				MessageAndMetadata<byte[], byte[]> message = streamIterator.next();				
				//String key = new String(message.key());
				String msg = new String(message.message());
				System.out.println(msg);
			}*/
		}
		
	}
	
	public void executeMsg(KafkaStream<byte[], byte[]> stream) {
		new MyStreamThread(stream).start();
	}
	
	private class MyStreamThread extends Thread{
		private KafkaStream<byte[], byte[]> stream;
		
		public MyStreamThread(KafkaStream<byte[], byte[]> stream){
			super();
			this.stream = stream;
		}
		
		@Override
		public void run(){
			ConsumerIterator<byte[], byte[]> streamIterator = stream.iterator();
			while(streamIterator.hasNext()) {
				MessageAndMetadata<byte[], byte[]> message = streamIterator.next();
				String topic = message.topic();
				int partition = message.partition();
				long offset = message.offset();
				//String key = new String(message.key());
				String msg = new String(message.message());
				System.out.println(topic + "\n" + msg + "\n" + partition + "\n" + offset);
			}
		}
	}
	
	public static void main(String[] args){
		String groupid = "group1";
		ConsumerTest consumer1 = new ConsumerTest(groupid, "myconsumer1", 1);
		consumer1.consume();
		//ConsumerTest consumer2 = new ConsumerTest("group1", "myconsumer1", 3);
		//consumer2.consume();
	}
}
