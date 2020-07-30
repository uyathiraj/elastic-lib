package com.yru.lib.es;

import java.util.Set;

import org.elasticsearch.common.unit.DistanceUnit;

public class ESGeoOperationRequest<T> extends ESOperationRequest<T> {

	public ESGeoOperationRequest(String index) {
		super(index);
	}

	private GeoPoint point;

	private Set<GeoPoint> pointSet;

	private double distance;

	private double radius;

	private DistanceUnit distanceUnit;

	public GeoPoint getPoint() {
		return point;
	}

	public void setPoint(GeoPoint point) {
		this.point = point;
	}

	public Set<GeoPoint> getPointSet() {
		return pointSet;
	}

	public void setPointSet(Set<GeoPoint> pointSet) {
		this.pointSet = pointSet;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public double getRadius() {
		return radius;
	}

	public void setRadius(double radius) {
		this.radius = radius;
	}

	public DistanceUnit getDistanceUnit() {
		return distanceUnit;
	}

	public void setDistanceUnit(DistanceUnit distanceUnit) {
		this.distanceUnit = distanceUnit;
	}

}
