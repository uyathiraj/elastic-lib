package com.yru.lib.es;

import java.util.List;

import com.yru.lib.exception.ESException;

public interface GeoESOperations {

	public <T> List<T> searchNearestPoints(ESGeoOperationRequest<?> esOperationRequest, Class<T> clazz)
			throws ESException;

	public <T> List<T> getAreaFromPoint(ESGeoOperationRequest<T> esOperationRequest, Class<T> clazz) throws ESException;

}
