package org.reactiveminds.txpipe.api;

/**
 * Exception to be thrown when a rollback is to be triggered
 * @author Sutanu_Dalui
 *
 */
public class CommitFailedException extends RuntimeException {

	public CommitFailedException(String msg, Throwable cause) {
		super(msg, cause);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
