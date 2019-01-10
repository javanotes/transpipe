package org.reactiveminds.txpipe.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Interface to be implemented for creating event recorders. Need to declare them as 
 * spring managed beans.
 * @author Sutanu_Dalui
 * @see RollbackEventRecorder 
 * @see CommitEventRecorder
 */
public interface EventRecorder {
	/**
	 * 
	 * @param record
	 */
	void record(EventRecord record);
	Logger eventLogger = LoggerFactory.getLogger("EventLog");
}
