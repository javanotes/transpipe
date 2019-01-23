package org.reactiveminds.txpipe.err;

import org.springframework.beans.factory.BeanInitializationException;

public class TxPipeIntitializationException extends BeanInitializationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3045658093707971928L;

	public TxPipeIntitializationException(String msg) {
		super(msg);
	}

	public TxPipeIntitializationException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
