package com.yru.lib.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yru.lib.exception.ESException;
import com.yru.lib.util.LibUtils;

public class ESOperationService implements ESOperation {

	private ESDocumentClient esDocumentClient;

	Logger logger = Logger.getLogger("test");

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

		BoolQueryBuilder finalQuery = null;
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
		finalQuery = QueryBuilders.boolQuery();
		for (QueryBuilder builder : querySet) {
			finalQuery.must(builder);
		}

		SearchSourceBuilder searchQuery = SearchSourceBuilder.searchSource().query(finalQuery);

		try {
			ESResponse<List<T>> response = esDocumentClient.searchDocument("employee", searchQuery, (map) -> {
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

}
