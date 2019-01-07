package org.reactiveminds.txpipe.err;

import org.springframework.core.NestedRuntimeException;
/**
 * Exception to be thrown when a rollback is to be triggered
 * @author Sutanu_Dalui
 *
 */
public class CommitFailedException extends NestedRuntimeException {

	public CommitFailedException(String msg, Throwable cause) {
		super(msg, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
