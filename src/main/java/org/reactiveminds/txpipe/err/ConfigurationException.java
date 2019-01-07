package org.reactiveminds.txpipe.err;

import org.springframework.beans.factory.BeanCreationException;

public class ConfigurationException extends BeanCreationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConfigurationException(String msg) {
		super(msg);
	}

	public ConfigurationException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
