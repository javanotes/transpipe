package org.reactiveminds.txpipe.err;

import org.springframework.beans.factory.BeanInitializationException;

public class IntitializationException extends BeanInitializationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3045658093707971928L;

	public IntitializationException(String msg) {
		super(msg);
	}

	public IntitializationException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
