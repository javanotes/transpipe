package org.reactiveminds.txpipe.utils;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicLong;

public class ONotificationManager extends Observable {

	private class O implements Observer{
		private O(ONotificationListener listener) {
			super();
			this.listener = listener;
			this.count = counter.incrementAndGet();
		}
		private O(long count) {
			super();
			this.count = count;
		}
		private ONotificationListener listener;
		@Override
		public void update(Observable o, Object arg) {
			if(arg instanceof ONotification) {
				listener.onNotify((ONotification) arg);
			}
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + (int) (count ^ (count >>> 32));
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			O other = (O) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (count != other.count)
				return false;
			return true;
		}
		private final long count;
		private ONotificationManager getOuterType() {
			return ONotificationManager.this;
		}
	}
	private final AtomicLong counter = new AtomicLong();
	/**
	 * Adds a listener to this manager
	 * @param listener
	 * @return listener id
	 */
	public long attach(ONotificationListener listener) {
		O o = new O(listener);
		addObserver(o);
		return o.count;
	}
	/**
	 * Removes a listener from this manager
	 * @param id
	 */
	public void detach(long id) {
		deleteObserver(new O(id));
	}
	/**
	 * 
	 * @param note
	 */
	public void notify(ONotification note) {
		setChanged();
		notifyObservers(note);
	}
}
