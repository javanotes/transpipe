package org.reactiveminds.txpipe.err;

import org.springframework.core.NestedRuntimeException;

public class TxPipeRuntimeException extends NestedRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TxPipeRuntimeException(String msg) {
		super(msg);
	}

	public TxPipeRuntimeException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
