package org.reactiveminds.txpipe.err;

public class ServiceDiscoveryException extends TxPipeRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ServiceDiscoveryException(String msg) {
		super(msg);
	}

	public ServiceDiscoveryException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
