package CornersKafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafkademo.KafkaProperties;

public class WriteCornersConsumer {
	private ConsumerConfig config;
    private String topic;
    private int partitionsNum;
    private MessageExecutor executor;
    private ConsumerConnector connector;
    private ExecutorService threadPool;
    public WriteCornersConsumer(String topic,int partitionsNum,MessageExecutor executor) throws Exception{
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
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
    	WriteCornersConsumer consumer = null;
    	String uri = "hdfs://localhost:9000/";
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), config);	
		FSDataOutputStream os = fs.create(new Path("/corners.log"));
		
      MessageExecutor executor = new MessageExecutor() {             
           public void execute(String message) {
                try {
                		JSONObject jsonStr = new JSONObject(message);
                		String filePath = (String) jsonStr.get("filePath") + "/part-r-00000";
            			FSDataInputStream in = fs.open(new Path(filePath));
            			BufferedReader bis;
            			bis = new BufferedReader(new InputStreamReader(in,"GBK"));
            			String tmp = null;      //every line 
            			String ftmp = null;		//first line
            			String ltmp = null;		//last line
            			
            			ftmp = bis.readLine();
            			while((tmp = bis.readLine()) != null){
            				ltmp = tmp;
            			}
            		
            			if(filePath.contains("LR")){	
            				String[] corners1 = ftmp.split("\t");				
            				String[] corners2 = ltmp.split("\t");
            				System.out.println(corners1[0] + "," + corners1[1]);
            				System.out.println(corners2[0] + "," + corners2[1]);
            				
            					
            				os.write(("Left: "+ corners1[0] + ", " + corners1[1] + "\n"
            							+ "Right: " + corners2[0] + ", " + corners2[1] + "\n").getBytes());
            				
            			}
            			else if(filePath.contains("NS")){			
            				String[] corners1 = ftmp.split("\t");
            				String[] corners2 = ltmp.split("\t");
            				System.out.println(corners1[1] + "," + corners1[0]);
            				System.out.println(corners2[1] + "," + corners2[0]);
            				try {
            					os.write(("North: "+ corners1[1] + ", " + corners1[0] + 
            							"\n" + "South: " + corners2[1] + ", " + corners2[0] + "\n").getBytes());
            				} catch (IOException e) {
            					// TODO Auto-generated catch block
            					e.printStackTrace();
            				}
            			}			
            		} catch (UnsupportedEncodingException e1) {
            			// TODO Auto-generated catch block
            			e1.printStackTrace();
            		} catch (IOException e) {
            			// TODO Auto-generated catch block
            			e.printStackTrace();
            		} catch (JSONException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}	
                	
                	               
                }
            };
         consumer = new WriteCornersConsumer("corners-topic", 2, executor);
         consumer.start();
    }

}
