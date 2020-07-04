package com.yru.lib.es;

import java.util.Set;

import org.elasticsearch.geo.geometry.Point;

public class ESGeoOperationRequest<T> extends ESOperationRequest<T> {

	public ESGeoOperationRequest(String index) {
		super(index);
	}

	private Point point;
	

	private Set<Point> pointSet;

	private double distance;

	public Point getPoint() {
		return point;
	}

	public void setPoint(Point point) {
		this.point = point;
	}

	public Set<Point> getPointSet() {
		return pointSet;
	}

	public void setPointSet(Set<Point> pointSet) {
		this.pointSet = pointSet;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	
	

}
