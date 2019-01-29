package org.reactiveminds.txpipe.err;

public class DataValidationException extends TxPipeRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DataValidationException(String s) {
		super(s);
	}

	public DataValidationException(String message, IllegalArgumentException cause) {
		super(message, cause);
	}

}
