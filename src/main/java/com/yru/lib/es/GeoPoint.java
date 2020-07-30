package com.yru.lib.es;

public class GeoPoint {

	private double lat;

	private double lon;

	public GeoPoint() {

	}

	public GeoPoint(double lattitude, double longitude) {
		super();
		this.lat = lattitude;
		this.lon = longitude;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lattitude) {
		this.lat = lattitude;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double longitude) {
		this.lon = longitude;
	}

	public double[] getAsArray() {
		double[] array = new double[2];
		array[0] = this.lon;
		array[1] = this.lat;
		return array;
	}

}
