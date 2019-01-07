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
	
	/**
	 * Base class to create a rollback event {@linkplain EventRecorder}
	 * @author Sutanu_Dalui
	 *
	 */
	public static abstract class RollbackEventRecorder implements EventRecorder{
		/**
		 * Do on new record received
		 * @param record
		 */
		protected abstract void onRecord(EventRecord record);
		@Override
		public void record(EventRecord record) {
			record.setRollback(true);
			onRecord(record);
		}
	}
	/**
	 * Base class to create a commit event {@linkplain EventRecorder}
	 * @author Sutanu_Dalui
	 *
	 */
	public static abstract class CommitEventRecorder extends RollbackEventRecorder{
		/**
		 * Do on new record received
		 * @param record
		 */
		@Override
		public void record(EventRecord record) {
			record.setRollback(false);
			onRecord(record);
		}
	}
}
