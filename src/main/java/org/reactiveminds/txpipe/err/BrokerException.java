package org.reactiveminds.txpipe.err;

import org.springframework.core.NestedRuntimeException;

public class BrokerException extends NestedRuntimeException {

	public BrokerException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
