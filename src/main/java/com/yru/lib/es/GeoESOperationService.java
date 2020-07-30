package com.yru.lib.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yru.lib.exception.ESException;
import com.yru.lib.util.LibUtils;

public class GeoESOperationService implements GeoESOperations {

	private ESDocumentClient esDocumentClient;

	Logger logger = Logger.getLogger(getClass().getName());

	public GeoESOperationService(String host, int port) {
		logger.info("Initializing Elastic search ");
		esDocumentClient = new ESDocumentClient(host, port);
	}

	@Override
	public <T> List<T> searchNearestPoints(ESGeoOperationRequest<?> esOperationRequest, Class<T> clazz)
			throws ESException {

		GeoPoint point = esOperationRequest.getPoint();
		BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

		if (esOperationRequest.getMustTerms() != null) {
			for (ESQuery term : esOperationRequest.getMustTerms()) {
				TermQueryBuilder termQuery = QueryBuilders.termQuery(term.getKey(), term.getValue());
				finalQuery.must(termQuery);
			}
		}
		GeoDistanceQueryBuilder geoDistanceFilter = QueryBuilders.geoDistanceQuery(ESConstants.BUILDING_LATLONG_FIELD)
				.distance(esOperationRequest.getDistance(), DistanceUnit.KILOMETERS)
				.point(point.getLat(), point.getLon()).geoDistance(GeoDistance.PLANE);

		GeoDistanceSortBuilder distanceSort = SortBuilders
				.geoDistanceSort(ESConstants.BUILDING_LATLONG_FIELD, point.getLat(), point.getLon())
				.order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS).ignoreUnmapped(true);

		SearchSourceBuilder searchQuery = SearchSourceBuilder.searchSource().query(geoDistanceFilter).sort(distanceSort)
				.size(esOperationRequest.getCount()).query(finalQuery);
		try {
			ESResponse<List<T>> response = esDocumentClient.searchDocument(esOperationRequest.getIndex(), searchQuery,
					(map) -> {
						try {
							return LibUtils.convertMapToEntity(map, clazz);
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
						return null;
					});
			if (response.getStatus() == 1) {
				return response.getData();
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new ESException(e);
		}
		return new ArrayList<T>();

	}

	@Override
	public <T> List<T> getAreaFromPoint(ESGeoOperationRequest<T> esOperationRequest, Class<T> clazz)
			throws ESException {
		double radius = ESConstants.DEFAULT_RADIUS_FOR_CIRCLE;
		DistanceUnit distanceUnit = DistanceUnit.METERS;
		if (esOperationRequest.getRadius() > 0) {
			radius = esOperationRequest.getRadius();
		}
		if (esOperationRequest.getDistanceUnit() != null) {
			distanceUnit = esOperationRequest.getDistanceUnit();
		}
		GeoPoint point = esOperationRequest.getPoint();
		BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

		if (esOperationRequest.getMustTerms() != null) {
			for (ESQuery term : esOperationRequest.getMustTerms()) {
				TermQueryBuilder termQuery = QueryBuilders.termQuery(term.getKey(), term.getValue());
				finalQuery.must(termQuery);
			}
		}

		CircleBuilder circleBuilder = new CircleBuilder().center(point.getLon(), point.getLat()).radius(radius,
				distanceUnit);
		GeoShapeQueryBuilder geoPolygonQuery;
		try {

			geoPolygonQuery = QueryBuilders.geoIntersectionQuery(ESConstants.BUILDING_GEO_SHAPE_FIELD, circleBuilder)
					.ignoreUnmapped(true);

			BoolQueryBuilder query = QueryBuilders.boolQuery().filter(geoPolygonQuery).must(finalQuery);

			SearchSourceBuilder searchQuery = SearchSourceBuilder.searchSource().query(query);
			if (esOperationRequest.getDocValues() != null) {
				String[] includeFields = esOperationRequest.getDocValues()
						.toArray(new String[esOperationRequest.getDocValues().size()]);
				searchQuery.fetchSource(includeFields, null);
			}

			ESResponse<List<T>> result = esDocumentClient.searchDocument(esOperationRequest.getIndex(), searchQuery,
					(map) -> {
						try {
							return LibUtils.convertMapToEntity(map, clazz);
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
						return null;
					});

			return result.getData();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			throw new ESException(e1);
		}

	}

}
