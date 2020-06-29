package com.yru.lib.es;

import java.util.List;

import com.yru.lib.exception.ESException;

/**
 * 
 * @author yathiraj
 *
 */
public interface ESOperation {

	/*
	 * Add document to ES
	 */
	public void addDocument(ESOperationRequest<?> esOperationRequest) throws ESException;

	/*
	 * Update ES doc
	 */
	public void updateDocument(String id, ESOperationRequest<?> esOperationRequest) throws ESException;

	public void deleteDocument(ESOperationRequest<?> esOperationRequest) throws ESException;

	public <T> List<T> searchDocument(ESOperationRequest<?> esOperationRequest, Class<T> clazz) throws ESException;

}
