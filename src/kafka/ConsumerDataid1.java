package kafka;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafkademo.KafkaProperties;
import model.QuickViewData;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class ConsumerDataid1 extends BaseConsumer {
	public QuickViewData qvDataModel = new QuickViewData();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
	
	public ConsumerDataid1(String groupid, String consumerid, int partition) {
		super(groupid, consumerid, partition);
		
	}

	@Override
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
				consumeMsg(msg);
				System.out.println(topic + "\n" + msg + "\n" + partition + "\n" + offset);
			}
		}
	}
	
	public void consumeMsg(String msg) {
		try {
			JSONObject jsonStr = new JSONObject(msg);
			String fileName = (String) jsonStr.get("File Path");
			fileName += "/desc.xml";
			getXMLData(fileName, "GBK");
			getCorners(jsonStr);
			save(qvDataModel);
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void save(QuickViewData qvDataModel) {
		// TODO Auto-generated method stub
		System.out.println(qvDataModel.getDataId());
		System.out.println(qvDataModel.getEastLon() + "," + qvDataModel.getEastLat());
		System.out.println(qvDataModel.getWestLon() + "," + qvDataModel.getWestLat());
		System.out.println(qvDataModel.getNorthLon() + "," + qvDataModel.getNorthLat());
		System.out.println(qvDataModel.getSouthLon() + "," + qvDataModel.getSouthLat());
		System.out.println(qvDataModel.getBands());
	}

	private void getCorners(JSONObject jsonStr) {
		// TODO Auto-generated method stub
		double lon = 0;
		double lat = 0;
		String westPoint;
		try {
			westPoint = (String) jsonStr.get("West Point");
			lon = Double.valueOf(westPoint.substring(11, westPoint.indexOf("\t")));
			lat = Double.valueOf(westPoint.substring(westPoint.lastIndexOf(":")+1, westPoint.length()-1));
			qvDataModel.setWestLon(lon);
			qvDataModel.setWestLat(lat);
			
			String eastPoint = (String) jsonStr.get("East Point");
			lon = Double.valueOf(eastPoint.substring(11, eastPoint.indexOf("\t")));
			lat = Double.valueOf(eastPoint.substring(eastPoint.lastIndexOf(":")+1, eastPoint.length()-1));
			qvDataModel.setEastLon(lon);
			qvDataModel.setEastLat(lat);
			
			String northPoint = (String) jsonStr.get("North Point");
			lon = Double.valueOf(northPoint.substring(11, northPoint.indexOf("\t")));
			lat = Double.valueOf(northPoint.substring(northPoint.lastIndexOf(":")+1, northPoint.length()-1));
			qvDataModel.setNorthLon(lon);
			qvDataModel.setNorthLat(lat);
			
			String southPoint = (String) jsonStr.get("South Point");
			lon = Double.valueOf(southPoint.substring(11, southPoint.indexOf("\t")));
			lat = Double.valueOf(southPoint.substring(southPoint.lastIndexOf(":")+1, southPoint.length()-1));
			qvDataModel.setSouthLon(lon);
			qvDataModel.setSouthLat(lat);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String getXMLData(String fileName, String encoding) {

		if (!fileExist(fileName)) {
			return null;
		}

		String dataId = null;
		Element element = null;

		DocumentBuilder db = null;
		DocumentBuilderFactory dbf = null;
		try {
			
			dbf = DocumentBuilderFactory.newInstance();			
			db = dbf.newDocumentBuilder();

			InputStreamReader reader = new InputStreamReader(new FileInputStream(fileName), encoding);
			BufferedReader br = new BufferedReader(reader);
			String str = null;
			StringBuffer sb = new StringBuffer();

			while ((str = br.readLine()) != null) {
				sb.append(str);
				// System.out.println(str);
			}
			str = sb.toString();
			str = str.substring(str.indexOf("<BASE_INFO>"), str.indexOf("</BASE_INFO>") + 12);
			str = str.replace("BASE_INFO", "DataDesc");

			String timeValue = str.substring(str.indexOf("<START_TIME>") + 12, str.indexOf("</START_TIME>"));
			if (timeValue.indexOf("-") == 4 && (timeValue.indexOf(" ") == 10 || timeValue.indexOf(" ") == 9)) {
				qvDataModel.setStart_time(sdf.parse(str.substring(str.indexOf("<START_TIME>") + 12, 
						str.indexOf("</START_TIME>"))));
				qvDataModel.setStop_time(sdf.parse(str.substring(str.indexOf("<STOP_TIME>") + 11, 
						str.indexOf("</STOP_TIME>"))));
			} else {
				qvDataModel.setStart_time(null);
				qvDataModel.setStop_time(null);
			}

			str = str.replace("\r", "");
			str = str.replace(" ", "");
			str = str.replace("\t", "");
			str = str.replace("\n", "");
			str = str.trim();
			br.close();
			reader.close();
			InputStream is = new ByteArrayInputStream(str.getBytes());

			Document dt = db.parse(is);
			element = dt.getDocumentElement();
			System.out.println("t" + element.getNodeName());

			NodeList c = element.getChildNodes();
			for (int j = 0; j < c.getLength(); j++) {
				Node a = c.item(j).getFirstChild();
				String v = "";
				String n = "";
				if (a != null) {
					n = c.item(j).getNodeName();
					System.out.println(n);
					v = a.getNodeValue();
				}

				if (v != null) {
					System.out.println(v);
					if ("DATAID".equalsIgnoreCase(n)) {
						dataId = v;
						qvDataModel.setDataId(v);
						continue;
						// dd.setDataid(v);
					}
					if ("PROCESSING".equalsIgnoreCase(n)) {
						// dd.setProcessing(Integer.valueOf(v));
						qvDataModel.setProcessing(Integer.valueOf(v));
						continue;
					}
					if ("STATION_ID".equalsIgnoreCase(n)) {
						qvDataModel.setStation_id(v);
						continue;
						//station_id = v;
					}
					if ("SPACECRAFT_ID".equalsIgnoreCase(n)) {
						qvDataModel.setSpacecraft_id(v);
						//spacecraft_id = v;
						continue;
					}
					if ("SENSOR_ID".equalsIgnoreCase(n)) {
						qvDataModel.setSensor_id(v);
						//sensor_id = v;
						continue;
					}
					if ("RESAMPLE_RATE".equalsIgnoreCase(n)) {
						qvDataModel.setResample_rate(Integer.valueOf(v));
						//resample_rate = Integer.valueOf(v);
						continue;
					}

					if ("ORBIT_NUM".equalsIgnoreCase(n)) {
						qvDataModel.setOrbit_num(Integer.valueOf(v));
						//orbit_num = Integer.valueOf(v);
						continue;
					}
					if ("COLUMNS".equalsIgnoreCase(n)) {
						qvDataModel.setColumns(Integer.valueOf(v));
						//columns = Integer.valueOf(v);
						continue;
					}
					if ("LINES".equalsIgnoreCase(n)) {
						qvDataModel.setLines(Integer.valueOf(v));
						//lines = Integer.valueOf(v);
						continue;
					}
					if ("BANDS".equalsIgnoreCase(n)) {
						qvDataModel.setBands(Integer.valueOf(v));
						//bands = Integer.valueOf(v);
						continue;
					}
					if ("PYRAMID_LEVELS".equalsIgnoreCase(n)) {
						qvDataModel.setPyramid_levels(Integer.valueOf(v));
						//pyramid_levels = Integer.valueOf(v);
						continue;
					}
					if ("PYRAMID_LEVELS_OFFSET".equalsIgnoreCase(n)) {
						qvDataModel.setPyramid_levels_offset(Integer.valueOf(v));
						//pyramid_levels_offset = Integer.valueOf(v);
						continue;
					}
					if ("IMAGE_TILE_SIDE".equalsIgnoreCase(n)) {
						qvDataModel.setImage_tile_side(Integer.valueOf(v));
						//image_tile_side = Integer.valueOf(v);
						continue;
					}
					if ("IFCITY".equalsIgnoreCase(n)) {
						qvDataModel.setIfcity(Integer.valueOf(v));
						//ifcity = Integer.valueOf(v);
						continue;
					}
					if ("IFPROVINCE".equalsIgnoreCase(n)) {
						qvDataModel.setIfprovince(Integer.valueOf(v));
						//ifprovince = Integer.valueOf(v);
						continue;
					}
					if ("IFAUXINFO".equalsIgnoreCase(n)) {
						qvDataModel.setIfauxinfo(Integer.valueOf(v));
						//ifauxinfo = Integer.valueOf(v);
						continue;
					}
					if ("IFEPH".equalsIgnoreCase(n)) {
						qvDataModel.setIfeph(Integer.valueOf(v));
						//ifeph = Integer.valueOf(v);
						continue;
					}
					if ("IFNADIR".equalsIgnoreCase(n)) {
						qvDataModel.setIfnadir(Integer.valueOf(v));
						//ifnadir = Integer.valueOf(v);
						continue;
					}
					if ("GRID_TILE_SIDE".equalsIgnoreCase(n)) {
						qvDataModel.setGrid_tile_side(Integer.valueOf(v));
						//grid_tile_side = Integer.valueOf(v);
						continue;
					}

					if ("vectrevised".equalsIgnoreCase(n)) {
						qvDataModel.setVectrevised(Boolean.valueOf(v));
						//vectrevised = Boolean.valueOf(v);
						continue;
					}
				}
				// System.out.println(a.getNodeName());
				// System.out.println(a.getNodeValue());

			}
			// daodd.save(dd);
			// System.out.println(dd.getDataid());

		}

		catch (Exception e) {
			e.printStackTrace();
		}

		return dataId;

	}
	
	private boolean fileExist(String fileName) {
		// TODO Auto-generated method stub
		File file = new File(fileName);
		if (file.exists()) {
			return true;
		}
		return false;
	}

	@Test
	public void testAbstract(){
		String groupid = "group1";
		String consumerid = "consumer1";
		int partition = 1;
		String topic = KafkaProperties.topic;
		ConsumerDataid1 consumer = new ConsumerDataid1(groupid, consumerid, partition);
		consumer.consume(topic, partition);
	}

	public static void main(String[] args) {
		String groupid = "group1";
		String consumerid = "consumer1";
		int partition = 1;
		String topic = KafkaProperties.topic;
		ConsumerDataid1 consumer = new ConsumerDataid1(groupid, consumerid, partition);
		consumer.consume(topic, partition);
	}
}
