package com.yru.lib.es;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yru.lib.util.LibUtils;

public class ESOperationRequest<T> {

	private String index;

	private ESDocument document;

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
		ESDocument document = new ESDocument();
		try {
			Map<String, Object> map = LibUtils.convertEntityToMap(entity);
			document.setProperties(map);
			document.setId(id);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		this.document = document;
	}

}
