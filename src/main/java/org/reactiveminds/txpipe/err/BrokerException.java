package org.reactiveminds.txpipe.err;

public class BrokerException extends TxPipeRuntimeException {

	public BrokerException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public BrokerException(String msg) {
		super(msg);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
