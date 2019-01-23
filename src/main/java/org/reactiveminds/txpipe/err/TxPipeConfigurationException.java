package org.reactiveminds.txpipe.err;

import org.springframework.beans.factory.BeanCreationException;

public class TxPipeConfigurationException extends BeanCreationException {

	private static final long serialVersionUID = 1L;

	public TxPipeConfigurationException(String msg) {
		super(msg);
	}

	public TxPipeConfigurationException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
