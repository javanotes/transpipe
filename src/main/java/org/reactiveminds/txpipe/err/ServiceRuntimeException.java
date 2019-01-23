package org.reactiveminds.txpipe.err;

public class ServiceRuntimeException extends TxPipeRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ServiceRuntimeException(String msg) {
		super(msg);
	}

	public ServiceRuntimeException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
