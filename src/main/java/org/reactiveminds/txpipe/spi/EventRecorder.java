package org.reactiveminds.txpipe.spi;

/**
 * Interface to be implemented for creating event recorders. Need to declare them as 
 * spring managed beans.
 * @author Sutanu_Dalui
 * @see RollbackEventRecorder 
 * @see CommitEventRecorder
 */
public interface EventRecorder {
	/**
	 * record based on the underlying implementation.
	 * @param record
	 */
	void record(EventRecord record);
	
}
