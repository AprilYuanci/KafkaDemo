package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafkademo.KafkaProperties;

public abstract class BaseConsumer {

	public String groupid = KafkaProperties.groupId;
	public String consumerid = KafkaProperties.groupId;
	public int threadPerTopic = 0;
	Properties props = new Properties();
	
	public BaseConsumer(String groupid, String consumerid, int partition) {
		props.put("group.id", groupid);
		props.put("consumer.id", consumerid);
		props.put("zookeeper.connect", KafkaProperties.zkConnect);
		props.put("zookeeper.session.timeout.ms", "60000");
		props.put("zookeeper.sync.time.ms", "2000");
	}
	
	public void consume(String topic, int threadPerTopic) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threadPerTopic);
		
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);
		
		for(KafkaStream<byte[], byte[]> stream: streams.get(topic)) {
			executeMsg(stream);
		}
	}
	
	public abstract void executeMsg(KafkaStream<byte[], byte[]> stream);
}
