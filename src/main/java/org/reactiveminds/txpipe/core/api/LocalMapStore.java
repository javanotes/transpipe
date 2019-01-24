package org.reactiveminds.txpipe.core.api;

import org.reactiveminds.txpipe.utils.SelfExpiringMap;
/**
 * A Map store implementing self expiration of records. Records will be persistent as configured.
 * @author Sutanu_Dalui
 *
 */
public interface LocalMapStore extends SelfExpiringMap<String, String> {

}
