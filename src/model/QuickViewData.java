package model;

import java.util.Date;

public class QuickViewData {

	private String dataId;
	private String station_id = "";
	private String spacecraft_id = "";
	private String sensor_id = "";
	private int processing;
	private int resample_rate;
	private Date start_time;
	private Date stop_time;
	private double westLon;
	private double westLat;
	private double eastLon;
	private double eastLat;
	private double northLon;
	private double northLat;
	private double southLon;
	private double southLat;
	private int orbit_num;
	private int columns;
	private int lines;
	private int bands;
	private int pyramid_levels;
	private int pyramid_levels_offset;
	private int image_tile_side;
	private int ifcity;
	private int ifprovince;
	private int ifauxinfo;
	private int ifeph;
	private int ifnadir;
	private int grid_tile_side;
	private int maxX = 0;
	private int maxY = 0;
	private int minX;
	private int minY;
	private boolean vectrevised;
	private String dataPath;
	private String timeInsert;
	private double cloudPercent;
	public int i =0;
	
	public String getDataId() {
		return dataId;
	}
	public void setDataId(String dataId) {
		this.dataId = dataId;
	}
	public String getStation_id() {
		return station_id;
	}
	public void setStation_id(String station_id) {
		this.station_id = station_id;
	}
	public String getSpacecraft_id() {
		return spacecraft_id;
	}
	public void setSpacecraft_id(String spacecraft_id) {
		this.spacecraft_id = spacecraft_id;
	}
	public String getSensor_id() {
		return sensor_id;
	}
	public void setSensor_id(String sensor_id) {
		this.sensor_id = sensor_id;
	}
	public int getProcessing() {
		return processing;
	}
	public void setProcessing(int processing) {
		this.processing = processing;
	}
	public int getResample_rate() {
		return resample_rate;
	}
	public void setResample_rate(int resample_rate) {
		this.resample_rate = resample_rate;
	}
	public Date getStart_time() {
		return start_time;
	}
	public void setStart_time(Date start_time) {
		this.start_time = start_time;
	}
	public Date getStop_time() {
		return stop_time;
	}
	public void setStop_time(Date stop_time) {
		this.stop_time = stop_time;
	}
	
	public int getOrbit_num() {
		return orbit_num;
	}
	public void setOrbit_num(int orbit_num) {
		this.orbit_num = orbit_num;
	}
	public int getColumns() {
		return columns;
	}
	public void setColumns(int columns) {
		this.columns = columns;
	}
	public int getLines() {
		return lines;
	}
	public void setLines(int lines) {
		this.lines = lines;
	}
	public int getBands() {
		return bands;
	}
	public void setBands(int bands) {
		this.bands = bands;
	}
	public int getPyramid_levels() {
		return pyramid_levels;
	}
	public void setPyramid_levels(int pyramid_levels) {
		this.pyramid_levels = pyramid_levels;
	}
	public int getPyramid_levels_offset() {
		return pyramid_levels_offset;
	}
	public void setPyramid_levels_offset(int pyramid_levels_offset) {
		this.pyramid_levels_offset = pyramid_levels_offset;
	}
	public int getImage_tile_side() {
		return image_tile_side;
	}
	public void setImage_tile_side(int image_tile_side) {
		this.image_tile_side = image_tile_side;
	}
	public int getIfcity() {
		return ifcity;
	}
	public void setIfcity(int ifcity) {
		this.ifcity = ifcity;
	}
	public int getIfprovince() {
		return ifprovince;
	}
	public void setIfprovince(int ifprovince) {
		this.ifprovince = ifprovince;
	}
	public int getIfauxinfo() {
		return ifauxinfo;
	}
	public void setIfauxinfo(int ifauxinfo) {
		this.ifauxinfo = ifauxinfo;
	}
	public int getIfeph() {
		return ifeph;
	}
	public void setIfeph(int ifeph) {
		this.ifeph = ifeph;
	}
	public int getIfnadir() {
		return ifnadir;
	}
	public void setIfnadir(int ifnadir) {
		this.ifnadir = ifnadir;
	}
	public int getGrid_tile_side() {
		return grid_tile_side;
	}
	public void setGrid_tile_side(int grid_tile_side) {
		this.grid_tile_side = grid_tile_side;
	}
	public int getMaxX() {
		return maxX;
	}
	public void setMaxX(int maxX) {
		this.maxX = maxX;
	}
	public int getMaxY() {
		return maxY;
	}
	public void setMaxY(int maxY) {
		this.maxY = maxY;
	}
	public int getMinX() {
		return minX;
	}
	public void setMinX(int minX) {
		this.minX = minX;
	}
	public int getMinY() {
		return minY;
	}
	public void setMinY(int minY) {
		this.minY = minY;
	}
	public boolean isVectrevised() {
		return vectrevised;
	}
	public void setVectrevised(boolean vectrevised) {
		this.vectrevised = vectrevised;
	}
	public String getDataPath() {
		return dataPath;
	}
	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}
	public String getTimeInsert() {
		return timeInsert;
	}
	public void setTimeInsert(String timeInsert) {
		this.timeInsert = timeInsert;
	}
	public double getWestLon() {
		return westLon;
	}
	public void setWestLon(double westLon) {
		this.westLon = westLon;
	}
	public double getWestLat() {
		return westLat;
	}
	public void setWestLat(double westLat) {
		this.westLat = westLat;
	}
	public double getEastLon() {
		return eastLon;
	}
	public void setEastLon(double eastLon) {
		this.eastLon = eastLon;
	}
	public double getEastLat() {
		return eastLat;
	}
	public void setEastLat(double eastLat) {
		this.eastLat = eastLat;
	}
	public double getNorthLon() {
		return northLon;
	}
	public void setNorthLon(double northLon) {
		this.northLon = northLon;
	}
	public double getNorthLat() {
		return northLat;
	}
	public void setNorthLat(double northLat) {
		this.northLat = northLat;
	}
	public double getSouthLon() {
		return southLon;
	}
	public void setSouthLon(double southLon) {
		this.southLon = southLon;
	}
	public double getSouthLat() {
		return southLat;
	}
	public void setSouthLat(double southLat) {
		this.southLat = southLat;
	}
	public double getCloudPercent() {
		return cloudPercent;
	}
	public void setCloudPercent(double cloudPercent) {
		this.cloudPercent = cloudPercent;
	}
	
}
