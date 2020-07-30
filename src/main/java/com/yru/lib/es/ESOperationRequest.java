package com.yru.lib.es;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yru.lib.util.LibUtils;

public class ESOperationRequest<T> {

	private String index;

	private ESDocument document;

	private List<ESDocument> bulkDocument;

	private Set<String> docValues;

	private int count;

	private Set<ESQuery> mustTerms;

	private Set<ESQuery> searchTerms;

	public ESOperationRequest(String index) {
		this.index = index;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public ESDocument getDocument() {
		return document;
	}

	public void setDocument(ESDocument document) {
		this.document = document;
	}

	public Set<ESQuery> getMustTerms() {
		return mustTerms;
	}

	public void setMustTerms(Set<ESQuery> mustTerms) {
		this.mustTerms = mustTerms;
	}

	public Set<ESQuery> getSearchTerms() {
		return searchTerms;
	}

	public void setSearchTerms(Set<ESQuery> searchTerms) {
		this.searchTerms = searchTerms;
	}

	public Set<String> getDocValues() {
		return docValues;
	}

	public void setDocValues(Set<String> docValues) {
		this.docValues = docValues;
	}

	public Set<ESQuery> addMustTerm(ESQuery term) {
		if (this.mustTerms == null) {
			this.mustTerms = new HashSet<>();
		}
		this.mustTerms.add(term);
		return this.mustTerms;
	}

	public Set<ESQuery> addSearchTerm(ESQuery term) {
		if (this.searchTerms == null) {
			this.searchTerms = new HashSet<>();
		}
		this.searchTerms.add(term);
		return this.searchTerms;
	}

	public void buildESDocument(String id, T entity) {

		this.document = buildESDoc(id, entity);
	}

	private ESDocument buildESDoc(String id, T entity) {
		ESDocument document = new ESDocument();
		try {
			Map<String, Object> map = LibUtils.convertEntityToMap(entity);
			document.setProperties(map);
			document.setId(id);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return document;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public List<ESDocument> getBulkDocument() {
		return bulkDocument;
	}

	public void setBulkDocument(List<ESDocument> bulkDocument) {
		this.bulkDocument = bulkDocument;
	}

	public ESOperationRequest<T> buildAndAddDoc(String id, T entity) {
		ESDocument doc = buildESDoc(id, entity);
		if (this.bulkDocument == null) {
			this.bulkDocument = new ArrayList<>();
		}
		this.bulkDocument.add(doc);
		return this;
	}

	public ESOperationRequest<?> addDocValue(String docValue) {
		if (this.docValues == null) {
			this.docValues = new HashSet<>();
		}
		this.docValues.add(docValue);
		return this;
	}

}
