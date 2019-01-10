package org.reactiveminds.txpipe.utils;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Observable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * A simple utility class which can run as a timer and notify observers of certain chronological
 * events (hourly change, minute change etc).
 * @author Sutanu_Dalui
 * @see HourlyNotifier
 * @see DailyNotifier
 * @see MinuteNotifier
 */
public abstract class TimeChangeNotifier extends Observable implements Runnable{

	private ScheduledExecutorService runningThread;
	protected Date lastRun;
	TimeChangeNotifier() {
		runningThread = Executors.newSingleThreadScheduledExecutor();
	}
	protected abstract Calendar nextTriggerOn();
	protected boolean hasConditionMet() {
		return lastRun != null && new Date().after(lastRun);
	}
	@Override
	public void run() {
        if(hasConditionMet()) {
        	setChanged();
        	notifyObservers();
        }
		lastRun = new Date();
        runningThread.schedule(this, nextTriggerOn().getTimeInMillis()-lastRun.getTime(), TimeUnit.MILLISECONDS);
	}
	public void start() {
		runningThread.execute(this);
	}
	public void stop() {
		if (runningThread != null) {
			runningThread.shutdownNow();
		}
	}
	/**
	 * A timer to notify on hour changes.
	 * @author Sutanu_Dalui
	 *
	 */
	public static class HourlyNotifier extends TimeChangeNotifier{

		public HourlyNotifier() {
			super();
		}
		@Override
		protected Calendar nextTriggerOn() {
			Calendar now = new GregorianCalendar();
	        now.add(Calendar.HOUR_OF_DAY, 1);      
	        now.set(Calendar.MINUTE, 0);
	        now.set(Calendar.SECOND, 0);
	        now.set(Calendar.MILLISECOND, 0);
			return now;
		}
		@Override
		protected boolean hasConditionMet() {
			if (lastRun != null) {
				Calendar c = new GregorianCalendar();
				c.setTime(lastRun);
				Calendar n = new GregorianCalendar();
				return (n.get(Calendar.HOUR_OF_DAY) > c.get(Calendar.HOUR_OF_DAY) || n.after(c));
			}
			return false;
		}
	}
	/**
	 * A timer to notify on day changes.
	 * @author Sutanu_Dalui
	 *
	 */
	public static class DailyNotifier extends TimeChangeNotifier{

		public DailyNotifier() {
			super();
		}
		@Override
		protected Calendar nextTriggerOn() {
			Calendar now = new GregorianCalendar();
			now.add(Calendar.DAY_OF_YEAR, 1); 
	        now.set(Calendar.HOUR_OF_DAY, 0);      
	        now.set(Calendar.MINUTE, 0);
	        now.set(Calendar.SECOND, 0);
	        now.set(Calendar.MILLISECOND, 0);
			return now;
		}
		@Override
		protected boolean hasConditionMet() {
			if (lastRun != null) {
				Calendar c = new GregorianCalendar();
				c.setTime(lastRun);
				Calendar n = new GregorianCalendar();
				return (n.get(Calendar.DAY_OF_YEAR) > c.get(Calendar.DAY_OF_YEAR) || n.after(c));
			}
			return false;
		}
	}
	/**
	 * A timer to notify on minute changes.
	 * @author Sutanu_Dalui
	 *
	 */
	public static class MinuteNotifier extends TimeChangeNotifier{

		public MinuteNotifier() {
			super();
		}
		@Override
		protected Calendar nextTriggerOn() {
			Calendar now = new GregorianCalendar();
	        now.add(Calendar.MINUTE, 1);
	        now.set(Calendar.SECOND, 0);
	        now.set(Calendar.MILLISECOND, 0);
			return now;
		}
		@Override
		protected boolean hasConditionMet() {
			if (lastRun != null) {
				Calendar c = new GregorianCalendar();
				c.setTime(lastRun);
				Calendar n = new GregorianCalendar();
				return (n.get(Calendar.MINUTE) > c.get(Calendar.MINUTE) || n.after(c));
			}
			return false;
		}
	}
}
