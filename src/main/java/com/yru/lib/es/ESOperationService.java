package com.yru.lib.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yru.lib.exception.ESException;
import com.yru.lib.util.LibUtils;

public class ESOperationService implements ESOperation {

	private ESDocumentClient esDocumentClient;

	Logger logger = Logger.getLogger(getClass().getName());

	public ESOperationService(String host, int port) {
		logger.info("Initializing Elastic search ");
		esDocumentClient = new ESDocumentClient(host, port);
	}

	@Override
	public void addDocument(ESOperationRequest<?> esOperationRequest) throws ESException {
		try {
			esDocumentClient.addDocument(esOperationRequest.getIndex(), esOperationRequest.getDocument());
		} catch (Exception e) {
			e.printStackTrace();
			throw new ESException(e);
		}

	}

	@Override
	public void updateDocument(String id, ESOperationRequest<?> esOperationRequest) {
		esDocumentClient.updateDocument(esOperationRequest.getIndex(), esOperationRequest.getDocument());

	}

	@Override
	public void deleteDocument(ESOperationRequest<?> esOperationRequest) throws ESException {
		try {
			esDocumentClient.deleteDocument(esOperationRequest.getIndex(), esOperationRequest.getDocument().getId());
		} catch (IOException e) {
			e.printStackTrace();
			throw new ESException(e);
		}

	}

	@Override
	public <T> List<T> searchDocument(ESOperationRequest<?> esOperationRequest, Class<T> clazz) throws ESException {

		BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
		Set<QueryBuilder> querySet = new HashSet<>();
		if (esOperationRequest.getSearchTerms() != null) {

			for (ESQuery term : esOperationRequest.getSearchTerms()) {
				MatchPhrasePrefixQueryBuilder nameAutocompleteQuery = QueryBuilders
						.matchPhrasePrefixQuery(term.getKey(), term.getValue()).maxExpansions(20);
				querySet.add(nameAutocompleteQuery);
			}

		}
		if (esOperationRequest.getMustTerms() != null) {
			for (ESQuery term : esOperationRequest.getMustTerms()) {
				TermQueryBuilder termQuery = QueryBuilders.termQuery(term.getKey(), term.getValue());
				querySet.add(termQuery);
			}
		}

		for (QueryBuilder builder : querySet) {
			finalQuery.must(builder);
		}

		SearchSourceBuilder searchQuery = SearchSourceBuilder.searchSource().query(finalQuery).from(0)
				.size(esOperationRequest.getCount());

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
	public <T> List<T> searchNearestPoints(ESGeoOperationRequest<?> esOperationRequest, Class<T> clazz)
			throws ESException {
		Point point = esOperationRequest.getPoint();
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
				.order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS);

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
	public void addDocumentBulk(ESOperationRequest<?> esOperationRequest) {
		esDocumentClient.addBulkDocumentAsync(esOperationRequest.getIndex(), esOperationRequest.getBulkDocument(),
				new Consumer<ESResponse<?>>() {

					@Override
					public void accept(ESResponse<?> arg0) {
						arg0.getData();
						logger.info("Bulk upload success");

					}
				});

	}

	@Override
	public <T> T getDocument(String index, String id, Class<T> clazz) {
		logger.info("Bulk addition of documents");
		try {
			ESResponse<?> response = esDocumentClient.getDocumentById(index, id, (map) -> {
				try {
					return LibUtils.convertMapToEntity(map, clazz);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				return null;
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
