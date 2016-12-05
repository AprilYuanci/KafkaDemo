package kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import kafkademo.KafkaProperties;

class PointByLonLat{
	private double longitude;
	private double latitude;
	
	public double getLongitude() {
		return longitude;
	}

	public void setPoint(double longitude, double latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public PointByLonLat(double longitude, double latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}
	
	public boolean compareByLon(double longitude) {
		return this.longitude > longitude;
	}
	
	public boolean compareByLat(double latitude) {
		return this.latitude > latitude;
	}
	
	public String toString()
	{
		return "longitude : " + longitude + "\t latitude : " + latitude;
	}
}

public class CornersOfQuickView {
	private String filePathOfQuickView;
	private String topic = KafkaProperties.topic;
	private Producer<String, String> producer;
	
	private PointByLonLat north;
	private PointByLonLat south;
	private PointByLonLat west;
	private PointByLonLat east;
	
	{
		filePathOfQuickView = "/home/" + System.getProperty("user.name") + "/Data/" + "QuickView/";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);
	}
	
	private void initCorners(String gridPath, File firstGridFile) {
		try {
			BufferedReader bf = new BufferedReader(new FileReader(gridPath + "/" + firstGridFile.getName()));
			String str = bf.readLine();
			str = bf.readLine();
			String[]strList = str.split(" |\t");
			
			north = new PointByLonLat(Double.valueOf(strList[0]), Double.valueOf(strList[1]));
			east = new PointByLonLat(Double.valueOf(strList[0]), Double.valueOf(strList[1]));
			south = new PointByLonLat(Double.valueOf(strList[0]), Double.valueOf(strList[1]));
			west = new PointByLonLat(Double.valueOf(strList[0]), Double.valueOf(strList[1]));
			bf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void findCorners() throws IOException {
		File fileHandler = new File(filePathOfQuickView);
		File[] quickViews = fileHandler.listFiles();
		int level = 0;
		
		for(File f : quickViews)
		{
			String gridPath = filePathOfQuickView + f.getName() + "/grid/pyramid/level" + level;
			File[] gridFileHandler = (new File(gridPath)).listFiles();
			
			initCorners(gridPath, gridFileHandler[0]);
			
			for(File gridFileIter : gridFileHandler)
			{
				BufferedReader bf = new BufferedReader(new FileReader(gridPath + "/" + gridFileIter.getName()));
				String str = bf.readLine();
				String[] strList = str.split(" |\t");
				int columnNo = Integer.valueOf(strList[1]);
				while((str = bf.readLine()) != null) {
					strList = str.split(" |\t");
					for( int iter = 0; iter < columnNo; iter += 3 ) {
						if(south.compareByLat(Double.valueOf(strList[iter + 1])))
						{
							south.setPoint(Double.valueOf(strList[iter]),Double.valueOf(strList[iter + 1]));
						} else if(! north.compareByLat(Double.valueOf(strList[iter + 1])))
						{
							north.setPoint(Double.valueOf(strList[iter]),Double.valueOf(strList[iter + 1]));
						}
						
						if(west.compareByLon(Double.valueOf(strList[iter])))
						{
							west.setPoint(Double.valueOf(strList[iter]),Double.valueOf(strList[iter + 1]));
						} else if(! east.compareByLat(Double.valueOf(strList[iter + 1])))
						{
							east.setPoint(Double.valueOf(strList[iter]),Double.valueOf(strList[iter + 1]));
						}
					}
				}
				bf.close();
			}
			sendToMQ(f.getName());
		}
		producer.close();
	}
	
	private void sendToMQ(String fileName) {
		JSONObject json = new JSONObject();
		try {
			json.put("File Path", filePathOfQuickView + fileName);
			json.put("West Point", west.toString());
			json.put("East Point", east.toString());
			json.put("South Point", south.toString());
			json.put("North Point", north.toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		producer.send(new ProducerRecord<String, String>(topic, Integer.toString(fileName.hashCode()), json.toString()));
	}
	
	public static void main(String[] args) {
		CornersOfQuickView newQuickView = new CornersOfQuickView();
		try {
			newQuickView.findCorners();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("hai");
	}
}