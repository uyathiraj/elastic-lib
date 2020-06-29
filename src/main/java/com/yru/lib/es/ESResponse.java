package com.yru.lib.es;

import java.io.Serializable;

public class ESResponse<T> implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int status;
	private  String string;
	
	private T data;
	
	public ESResponse(int status) {
		this.status = status;
	}
	
	public ESResponse(T data) {
		this.status = 1;
		this.data = data;
	}
	
	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}
	
	
}
