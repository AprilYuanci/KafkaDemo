package CornersKafka;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import kafkademo.LogProducer;

public class FindEastWestMP {
	
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
	
	
	public void FindEastWestMP(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException{
		
		Job jobLR = new Job();
		jobLR.setJarByClass(FindEastWestMP.class);
		
		FileInputFormat.addInputPath(jobLR, new Path(inputPath));
		FileOutputFormat.setOutputPath(jobLR, new Path(outputPath));
		
		jobLR.setMapperClass(cornersLRMapper.class);
		jobLR.setReducerClass(cornersLRReducer.class);
		
		jobLR.setOutputKeyClass(DoubleWritable.class);
		jobLR.setOutputValueClass(DoubleWritable.class);
		
		System.exit(jobLR.waitForCompletion(true)?0:1);
	}
	
	public void corners(String input, String outputlr, String outputns) throws ClassNotFoundException, IOException, InterruptedException{
		Job jobLR = new Job();
		jobLR.setJarByClass(FindEastWestMP.class);
		
		/*FileInputFormat.addInputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/input_corners"));
		FileOutputFormat.setOutputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/output_cornersLR1"));*/
		
		FileInputFormat.addInputPath(jobLR, new Path(input));
		FileOutputFormat.setOutputPath(jobLR, new Path(outputlr));
		
		jobLR.setMapperClass(cornersLRMapper.class);
		jobLR.setReducerClass(cornersLRReducer.class);
		
		/*job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);*/
		jobLR.setOutputKeyClass(DoubleWritable.class);
		jobLR.setOutputValueClass(DoubleWritable.class);
		
		//String uri = "hdfs://localhost:9000/";
		//String uri = "hdfs://node1:9000/";
		/*String uri = "hdfs://localhost:9000/";
		Configuration config = new Configuration();
		FSDataOutputStream os = null;
		FileSystem fs = FileSystem.get(URI.create(uri), config);			
		os = fs.create(new Path("/corners.log"));
		
		if(jobLR.waitForCompletion(true)){
		
			writeCorners("hdfs://localhost:9000/testCorners/output_cornersLR/part-r-00000",os);
			
			writeCorners(fs, outputlr + "/part-r-00000",os);
			
			Job jobNS = new Job();
			jobNS.setJarByClass(cornersTest.class);
			
			FileInputFormat.addInputPath(jobNS, new Path("hdfs://localhost:9000/testCorners/input_corners"));
			FileOutputFormat.setOutputPath(jobNS, new Path("hdfs://localhost:9000/testCorners/output_cornersNS"));
			
			FileInputFormat.addInputPath(jobNS, new Path(input));
			FileOutputFormat.setOutputPath(jobNS, new Path(outputns));
			
			jobNS.setMapperClass(cornersNSMapper.class);			
			jobNS.setReducerClass(cornersNSReducer.class);
			
			jobNS.setOutputKeyClass(DoubleWritable.class);
			jobNS.setOutputValueClass(DoubleWritable.class);
			
			if(jobNS.waitForCompletion(true)){
				//writeCorners("hdfs://localhost:9000/testCorners/output_cornersNS/part-r-00000",os);
				writeCorners(fs, outputns + "/part-r-00000",os);
			}
			
		}
		
		try {
			os.flush();
			os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		/*if(jobLR.waitForCompletion(true)){
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
		}*/
		
		System.exit(jobLR.waitForCompletion(true)?0:1);
	}
	
	@Test
	public void test(){
		String input = "hdfs://localhost:9000/testCorners/input_corners1";
		String output = "hdfs://localhost:9000/testCorners/output_corners";
		try {
			//FindEastWestMP(input, output);
			corners(input, output, output);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
