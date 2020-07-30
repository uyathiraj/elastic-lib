package com.yru.lib.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

class ESDocumentClient {

	private String host;
	private int port;
	protected static RestHighLevelClient restClient;

	Logger logger = Logger.getLogger(getClass().getName());

	public ESDocumentClient(String host, int port) {
		this.host = host;
		this.port = port;
	}

	private void connect() {

		RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(host, port, "http"));
		restClient = new RestHighLevelClient(clientBuilder);
	}

	private void close() {
		if (restClient != null) {
			try {
				restClient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void addDocumentAsych(String index, ESDocument document, final Consumer<ESResponse<?>> listner) {
		IndexRequest indexRequest = new IndexRequest().source(document.getProperties())
				.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		connect();

		ActionListener<IndexResponse> actionListener = new ActionListener<IndexResponse>() {

			@Override
			public void onResponse(IndexResponse response) {
				close();

				listner.accept(new ESResponse<>(response));

			}

			@Override
			public void onFailure(Exception e) {
				listner.accept(new ESResponse<>(0));
				close();
			}

		};
		restClient.indexAsync(indexRequest, RequestOptions.DEFAULT, actionListener);

	}

	public ESResponse<?> addDocument(String index, ESDocument document) throws Exception {
		IndexRequest indexRequest = new IndexRequest(index, "_doc").id(document.getId())
				.source(document.getProperties()).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		indexRequest.setRefreshPolicy("wait_for");

		connect();
		IndexResponse response = null;
		try {
			response = restClient.index(indexRequest, RequestOptions.DEFAULT);

			return new ESResponse<>(response);
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception(e);
		} finally {
			close();
		}

	}

	public void addBulkDocumentAsync(String index, List<ESDocument> documents, final Consumer<ESResponse<?>> listner) {
		BulkRequest bulkRequest = new BulkRequest();
		for (ESDocument document : documents) {
			IndexRequest indexRequest = new IndexRequest(index, "_doc").id(document.getId())
					.source(document.getProperties());
			// indexRequest.setRefreshPolicy("wait_for");
			indexRequest.setRefreshPolicy(RefreshPolicy.NONE);
			indexRequest.timeout(new TimeValue(5, TimeUnit.MINUTES));
			bulkRequest.add(indexRequest);
		}

		ActionListener<BulkResponse> actionListener = new ActionListener<BulkResponse>() {

			@Override
			public void onResponse(BulkResponse response) {
				close();
				logger.info("Bulk upload success" + response.status());
				listner.accept(new ESResponse<>(response));

			}

			@Override
			public void onFailure(Exception e) {
				close();
				e.printStackTrace();
				listner.accept(new ESResponse<>(0));

			}

		};
		connect();
		restClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, actionListener);

	}

	public void updateDocumentAsync(String index, ESDocument document, final Consumer<ESResponse<?>> listner) {
		UpdateRequest updateRequest = new UpdateRequest(index, document.getId()).doc(document.getProperties())
				.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).retryOnConflict(3);

		ActionListener<UpdateResponse> actionListener = new ActionListener<UpdateResponse>() {

			@Override
			public void onResponse(UpdateResponse response) {
				close();
				listner.accept(new ESResponse<>(response));

			}

			@Override
			public void onFailure(Exception e) {
				close();
				listner.accept(new ESResponse<>(0));

			}

		};
		connect();
		restClient.updateAsync(updateRequest, RequestOptions.DEFAULT, actionListener);
	}

	public void updateDocument(String index, ESDocument document) {
		UpdateRequest updateRequest = new UpdateRequest(index, document.getId()).doc(document.getProperties())
				.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).retryOnConflict(3);

		connect();
		try {
			restClient.update(updateRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public ESResponse<?> deleteDocument(String index, String id) throws IOException {
		DeleteRequest deleteRequest = new DeleteRequest(index, id);
		connect();
		DeleteResponse response = restClient.delete(deleteRequest, RequestOptions.DEFAULT);
		close();
		return new ESResponse<>(response);
	}

	public <T> ESResponse<T> getDocumentById(String index, String id, Function<Map<String, Object>, T> fieldMapper)
			throws IOException {
		GetRequest getRequest = new GetRequest(index).id(id);
		connect();
		GetResponse response = restClient.get(getRequest, RequestOptions.DEFAULT);
		close();
		if (response.isExists()) {
			T t = fieldMapper.apply(response.getSourceAsMap());
			return new ESResponse<T>(t);
		}
		return new ESResponse<T>(0);
	}

	public <T> ESResponse<List<T>> searchDocument(String index, SearchSourceBuilder sourceBuilder,
			Function<Map<String, Object>, T> fieldMapper) throws IOException {
		SearchRequest searchRequest = new SearchRequest(index).source(sourceBuilder);
		connect();
		System.out.println(searchRequest.source().toString());
		SearchResponse response = restClient.search(searchRequest, RequestOptions.DEFAULT);
		close();
		List<T> result = new ArrayList<>();
		if (response.status() == RestStatus.OK) {
			for (SearchHit hit : response.getHits()) {
				T t = fieldMapper.apply(hit.getSourceAsMap());
				result.add(t);
			}
		}
		return new ESResponse<>(result);
	}

}
