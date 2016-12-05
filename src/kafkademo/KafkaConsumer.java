package kafkademo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafkademo.LogConsumer.MessageExecutor;
import kafkademo.LogConsumer.MessageRunner;

public class KafkaConsumer extends Thread {

	private final ConsumerConnector consumer;
	private final String topic;
	private MessageExecutor executor;
	private ConsumerConnector connector;
	private static ConsumerConfig config;
    private ExecutorService threadPool;
    private int partitionsNum;
	public KafkaConsumer(String topic,int partitionsNum,MessageExecutor executor){
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
		this.partitionsNum = partitionsNum;
		this.executor = executor;
	}
	
	private static ConsumerConfig createConsumerConfig(){
		Properties props = new Properties();
		props.put("zookeeper.connect", KafkaProperties.zkConnect);
		props.put("group.id", KafkaProperties.groupId);
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		config = new ConsumerConfig(props);
		return new ConsumerConfig(props);
	}
	
    class MessageRunner implements Runnable{
        private KafkaStream<byte[], byte[]> partition;
        
        MessageRunner(KafkaStream<byte[], byte[]> partition) {
            this.partition = partition;
        }
        
        public void run(){
            ConsumerIterator<byte[], byte[]> it = partition.iterator();
            while(it.hasNext()){
                MessageAndMetadata<byte[],byte[]> item = it.next();
                System.out.println("partiton:" + item.partition());
                System.out.println("offset:" + item.offset());
                executor.execute(new String(item.message()));//UTF-8
            }
        }
    }
	
	@Override
	public void run(){
		/*Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = 
				consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while(it.hasNext()){
			System.out.println("receive: " + new String(it.next().message()));
			try{
				sleep(3000);
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}*/
		
		connector = Consumer.createJavaConsumerConnector(config);
        Map<String,Integer> topics = new HashMap<String,Integer>();
        topics.put(topic, partitionsNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics);
        List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);
        threadPool = Executors.newFixedThreadPool(partitionsNum);
        for(KafkaStream<byte[], byte[]> partition : partitions){
            threadPool.execute(new MessageRunner(partition));
        } 
	}
}
