package CornersKafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import CornersKafka.cornersTest;

public class EastWestProducer {

	private Producer<String, String> producer;
	
	public EastWestProducer(){
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "127.0.0.1:9092");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}
	
	public void send(String topic, String message){
		if(topic == null || message == null){
			return;
		}
		
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic, message);
		producer.send(km);
	}
	
	public void send(String topic, ArrayList<String> messages){
		if(topic == null || messages == null){
			return;
		}
		if(messages.isEmpty()){
			return;
		}
		List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
      for(String entry : messages){       		
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic,entry);
            kms.add(km);
        }
      producer.send(kms);
	}
	
	public void close(){
		producer.close();
	}
	
	static class cornersLRMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();    
			line = line.replace("\t\t", "\t");
			
			String[] points = line.split("\t");
			if(points.length != 1){      //get rid of the first line
				for(int i = 0; i<points.length; i++){
					points[i] = points[i].trim();
					String[] point = points[i].split(" ");
					DoubleWritable lon = new DoubleWritable();
					lon.set(Double.valueOf(point[0])); 
					DoubleWritable lat = new DoubleWritable(Double.valueOf(point[1]));					
					context.write(lon, lat);
				}
			}			
		}
	}
	
	static class cornersLRReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>{
		public void reduce(DoubleWritable key, DoubleWritable values, Context context) 
		throws IOException, InterruptedException{
			
			context.write(key, values);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length != 2){
			System.out.println("usage: input_path_of_data output_path_of_data");
		}
		EastWestProducer producer = new EastWestProducer();
		Job jobLR = new Job();
		jobLR.setJarByClass(cornersTest.class);
		
		/*FileInputFormat.addInputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/input_corners"));
		FileOutputFormat.setOutputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/output_cornersLR1"));*/
		
		FileInputFormat.addInputPath(jobLR, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobLR, new Path(args[1]));
		
		jobLR.setMapperClass(cornersLRMapper.class);
		jobLR.setReducerClass(cornersLRReducer.class);
		
		/*job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);*/
		jobLR.setOutputKeyClass(DoubleWritable.class);
		jobLR.setOutputValueClass(DoubleWritable.class);
		
		if(jobLR.waitForCompletion(true)){
			try{
				//FindEastWestMP findEastWest = new FindEastWestMP();					
				//String eastwestFile = findEastWest.FindEastWestMP(args[0], args[1]);
				/*cornersTest corners = new cornersTest();
				corners.corners(args[0], args[1], args[1]);*/
				//JSONObject ewFile = new JSONObject(eastwestFile);
				producer.send("corners-topic", args[1]);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
		
	}
	
}
