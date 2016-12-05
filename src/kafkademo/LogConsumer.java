package kafkademo;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
public class LogConsumer {

    private ConsumerConfig config;
    private String topic;
    private int partitionsNum;
    private MessageExecutor executor;
    private ConsumerConnector connector;
    private ExecutorService threadPool;
    public LogConsumer(String topic,int partitionsNum,MessageExecutor executor) throws Exception{
        Properties properties = new Properties();
        /*File file = new File("consumer.properties");
		  FileInputStream fis = new FileInputStream(file);
        properties.load(fis);*/
        properties.put("zookeeper.connect", KafkaProperties.zkConnect);
        properties.put("group.id", KafkaProperties.groupId);
        properties.put("zookeeper.session.timeout.ms", "40000");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        config = new ConsumerConfig(properties);
        this.topic = topic;
        this.partitionsNum = partitionsNum;
        this.executor = executor;
    }
    
    public void start() throws Exception{
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

        
    public void close(){
        try{
            threadPool.shutdownNow();
        }catch(Exception e){
            //
        }finally{
            connector.shutdown();
        }
        
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
    
    interface MessageExecutor {
        
        public void execute(String message);
    }
    
    public static void testConsumer(){
    	LogConsumer consumer = null;
        try{
            MessageExecutor executor = new MessageExecutor() {               
                public void execute(String message) {
                		try {
								JSONObject jsonStr = new JSONObject(message);
								System.out.println("received:" + jsonStr.get("param1") + " & " + jsonStr.get("param2"));				
							} catch (JSONException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                }
            };
            consumer = new LogConsumer("test-topic", 2, executor);
            consumer.start();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
//            if(consumer != null){
//                consumer.close();
//            }
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        LogConsumer consumer = null;
        try{
            MessageExecutor executor = new MessageExecutor() {               
                public void execute(String message) {
                		try {
								JSONObject jsonStr = new JSONObject(message);
								System.out.println("received:" + jsonStr.get("param1") + " & " + jsonStr.get("param2"));				
							} catch (JSONException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                }
            };
            consumer = new LogConsumer("test-topic", 2, executor);
            consumer.start();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
//            if(consumer != null){
//                consumer.close();
//            }
        }

    }

}