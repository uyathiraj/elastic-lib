package com.yru.lib;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.yru.lib.es.ESDocument;
import com.yru.lib.es.ESOperationRequest;
import com.yru.lib.es.ESOperationService;
import com.yru.lib.es.ESQuery;
import com.yru.lib.exception.ESException;

public class ESTest {

	private static String host = "localhost";
	private static int port = 9200;

	public static void main(String[] args) throws IOException {

		ESDocument doc = new ESDocument("1234").addProperty("name", "Yathiraj").addProperty("age", 20);

		ESDocument doc2 = new ESDocument("456").addProperty("name", "Raj Umesh").addProperty("age", 29);

		ESOperationService client = new ESOperationService(host, port);

		/*
		 * try { client.addDocument("employee", doc); client.addDocument("employee",
		 * doc2); } catch (Exception e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 * 
		 * try { ESResponse<?> response = client.getDocumentById("employee", "1234");
		 * System.out.println(response.getData()); } catch (IOException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */
		ESOperationRequest request1 = new ESOperationRequest("employee");
		request1.setDocument(doc);
		try {
			client.addDocument(request1);
			
		} catch (ESException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		
		

		ESOperationRequest request = new ESOperationRequest("employee");

		request.addSearchTerm(new ESQuery("name", "yathi"));
		request.addMustTerm(new ESQuery("age", 20));
		List<Map> response;
		try {
			response = client.searchDocument(request, Map.class);
			System.out.println("Called " + response);
			for (Map d : response) {
				System.out.println(d);
			}
		} catch (ESException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
