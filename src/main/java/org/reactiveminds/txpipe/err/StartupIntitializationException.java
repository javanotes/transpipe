package org.reactiveminds.txpipe.err;

import org.springframework.beans.factory.BeanInitializationException;

public class StartupIntitializationException extends BeanInitializationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3045658093707971928L;

	public StartupIntitializationException(String msg) {
		super(msg);
	}

	public StartupIntitializationException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
