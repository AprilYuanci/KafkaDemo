package CornersKafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import kafkademo.LogProducer;

public class cornersTest {
	
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
	
	static class cornersNSMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable>{
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
					context.write(lat,lon);
				}
			}			
		}
	}
	
	static class cornersNSReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>{
		public void reduce(DoubleWritable key, DoubleWritable values, Context context) 
		throws IOException, InterruptedException{
			
			context.write(key, values);
		}
	}
	
	public static void writeCorners(FileSystem fs, String filePath, FSDataOutputStream os){
		
		//File lonFile = new File("/home/hadoop/Documents/output_corners/part-r-00000");	
		try {
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
				try {
					os.write(("Left: "+ corners1[0] + ", " + corners1[1] + "\n"
							+ "Right: " + corners2[0] + ", " + corners2[1] + "\n").getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
		}	
	}
	
	public void corners(String input, String outputlr, String outputns) throws ClassNotFoundException, IOException, InterruptedException{
		Job jobLR = new Job();
		jobLR.setJarByClass(cornersTest.class);
		
		/*FileInputFormat.addInputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/input_corners"));
		FileOutputFormat.setOutputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/output_cornersLR1"));*/
		
		FileInputFormat.addInputPath(jobLR, new Path(input));
		FileOutputFormat.setOutputPath(jobLR, new Path(outputlr));
		
		jobLR.setMapperClass(cornersLRMapper.class);
		jobLR.setReducerClass(cornersLRReducer.class);
		
		jobLR.setOutputKeyClass(DoubleWritable.class);
		jobLR.setOutputValueClass(DoubleWritable.class);
		
		System.exit(jobLR.waitForCompletion(true)?0:1);
	}
	
	@Test
	public void test(){
		String input = "hdfs://localhost:9000/testCorners/input_corners1";
		String outputlr = "hdfs://localhost:9000/testCorners/output_cornersLR";
		String outputns = "hdfs://localhost:9000/testCorners/output_cornersNS";
		try {
			corners(input, outputlr, outputns);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		if(args.length != 5){
			System.out.println("usage: input_path output_path_4_LR output_path_4_NS nodeName port");
			System.exit(1);
		}
		Job jobLR = new Job();
		jobLR.setJarByClass(cornersTest.class);
		
		FileInputFormat.addInputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/input_corners"));
		FileOutputFormat.setOutputPath(jobLR, new Path("hdfs://localhost:9000/testCorners/output_cornersLR"));
		
		FileInputFormat.addInputPath(jobLR, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobLR, new Path(args[1]));
		
		jobLR.setMapperClass(cornersLRMapper.class);
		jobLR.setReducerClass(cornersLRReducer.class);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		jobLR.setOutputKeyClass(DoubleWritable.class);
		jobLR.setOutputValueClass(DoubleWritable.class);
		
		//String uri = "hdfs://localhost:9000/";
		//String uri = "hdfs://node1:9000/";
		String uri = "hdfs://" + args[3] + ":" + args[4] + "/";
		Configuration config = new Configuration();
		FSDataOutputStream os = null;
		FileSystem fs = FileSystem.get(URI.create(uri), config);			
		os = fs.create(new Path("/corners.log"));
		
		if(jobLR.waitForCompletion(true)){
		
			//writeCorners("hdfs://localhost:9000/testCorners/output_cornersLR/part-r-00000",os);
			
			writeCorners(fs, args[1] + "/part-r-00000",os);
			
			Job jobNS = new Job();
			jobNS.setJarByClass(cornersTest.class);
			
			FileInputFormat.addInputPath(jobNS, new Path("hdfs://localhost:9000/testCorners/input_corners"));
			FileOutputFormat.setOutputPath(jobNS, new Path("hdfs://localhost:9000/testCorners/output_cornersNS"));
			
			FileInputFormat.addInputPath(jobNS, new Path(args[0]));
			FileOutputFormat.setOutputPath(jobNS, new Path(args[2]));
			
			jobNS.setMapperClass(cornersNSMapper.class);			
			jobNS.setReducerClass(cornersNSReducer.class);
			
			jobNS.setOutputKeyClass(DoubleWritable.class);
			jobNS.setOutputValueClass(DoubleWritable.class);
			
			if(jobNS.waitForCompletion(true)){
				//writeCorners("hdfs://localhost:9000/testCorners/output_cornersNS/part-r-00000",os);
				writeCorners(fs, args[2] + "/part-r-00000",os);
			}
			
		}
		
		try {
			os.flush();
			os.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.exit(jobLR.waitForCompletion(true)?0:1);
	}
}
