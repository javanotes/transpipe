package org.reactiveminds.txpipe.err;

import java.io.IOException;

public class DataSerializationException extends TxPipeRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public DataSerializationException(String message, IOException cause) {
		super(message, cause);
	}
	public DataSerializationException(IOException cause) {
		super("");
		initCause(cause);
	}
}
