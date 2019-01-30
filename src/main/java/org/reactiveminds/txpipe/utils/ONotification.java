package org.reactiveminds.txpipe.utils;

/**
 * Observer notification
 * @author Sutanu_Dalui
 *
 */
public final class ONotification{
	public static enum Type{FUTURE_SET, KEY_EXPIRED}
	/**
	 * 
	 * @param type
	 * @param object
	 */
	public ONotification(ONotification.Type type, Object object) {
		super();
		this.type = type;
		this.object = object;
	}
	public final ONotification.Type type;
	public final Object object;
}