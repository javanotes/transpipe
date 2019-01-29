package org.reactiveminds.txpipe.core.dto;

public class HttpResponse {
	public HttpResponse() {
		this(Code.OK, "");
	}

	public HttpResponse(Code code, String info) {
		this(code, info, "");
	}
	/**
	 * 
	 * @param code
	 * @param info
	 * @param txn_result
	 */
	public HttpResponse(Code code, String info, TransactionResult txn_result) {
		super();
		this.code = code;
		this.info = info;
		this.txn_result = txn_result;
	}

	public HttpResponse(Code code, String info, String detail) {
		super();
		this.code = code;
		this.info = info;
		this.detail = detail;
	}

	public Code getCode() {
		return code;
	}

	public void setCode(Code code) {
		this.code = code;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public TransactionResult getTxn_result() {
		return txn_result;
	}

	public void setTxn_result(TransactionResult txn_result) {
		this.txn_result = txn_result;
	}

	public static enum Code {
		OK, WARN, ERR
	}

	private Code code;
	private String info;
	private String detail;
	private TransactionResult txn_result;
}
