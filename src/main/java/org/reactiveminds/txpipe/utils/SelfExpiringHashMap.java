package org.reactiveminds.txpipe.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
// Adapted from https://gist.github.com/pcan/16faf4e59942678377e0
/**
 * The default implementation of a {@link SelfExpiringMap} that uses a {@link DelayQueue} to determine
 * expiration of next records.<p> Note : The notification execution happens in a cached pooled thread. So it should not
 * be used for long running/blocking tasks.
 * @author Sutanu_Dalui
 *
 * @param <K>
 * @param <V>
 */
public class SelfExpiringHashMap<K, V> extends Observable implements SelfExpiringMap<K, V>,Runnable {

    private final Map<ExpiringKey<K>, V> internalMap;
    private static ExecutorService notifThread;
    private final Map<K, ExpiringKey<K>> expiringKeys;

    /**
     * Holds the map keys using the given life time for expiration.
     */
    private final DelayQueue<ExpiringKey<K>> delayQueue = new DelayQueue<>();

    /**
     * The default max life time in milliseconds.
     */
    private final long maxLifeTimeMillis;

    static{
    	notifThread = Executors.newCachedThreadPool((r) -> {
    		Thread t = new Thread(r, "SelfExpiringMapNotifier");
    		t.setDaemon(true);
    		return t;
    	});
    }
    public SelfExpiringHashMap() {
    	this(Long.MAX_VALUE);
    }
    /**
     * 
     * @param supplier
     * @param defaultMaxLifeTimeMillis
     */
    public SelfExpiringHashMap(InternalMapSupplier<ExpiringKey<K>, V> supplier, long defaultMaxLifeTimeMillis) {
        internalMap = supplier.supply();
        expiringKeys = Collections.synchronizedMap(new WeakHashMap<K, ExpiringKey<K>>());
        this.maxLifeTimeMillis = defaultMaxLifeTimeMillis;
    }
    
    public SelfExpiringHashMap(long defaultMaxLifeTimeMillis) {
        internalMap = new ConcurrentHashMap<>();
        expiringKeys = Collections.synchronizedMap(new WeakHashMap<K, ExpiringKey<K>>());
        this.maxLifeTimeMillis = defaultMaxLifeTimeMillis;
    }

    public SelfExpiringHashMap(long defaultMaxLifeTimeMillis, int initialCapacity) {
        internalMap = new ConcurrentHashMap<>(initialCapacity);
        expiringKeys = Collections.synchronizedMap(new WeakHashMap<K, ExpiringKey<K>>(initialCapacity));
        this.maxLifeTimeMillis = defaultMaxLifeTimeMillis;
    }

    public SelfExpiringHashMap(long defaultMaxLifeTimeMillis, int initialCapacity, float loadFactor) {
        internalMap = new ConcurrentHashMap<>(initialCapacity, loadFactor);
        expiringKeys = Collections.synchronizedMap(new WeakHashMap<K, ExpiringKey<K>>(initialCapacity, loadFactor));
        this.maxLifeTimeMillis = defaultMaxLifeTimeMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return internalMap.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return internalMap.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
        return internalMap.containsKey(new ExpiringKey<>(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        return internalMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
        renewKey(key);
        return internalMap.get(new ExpiringKey<>(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value) {
        return this.put(key, value, maxLifeTimeMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public V put(K key, V value, long lifeTimeMillis) {
        ExpiringKey<K> delayedKey = new ExpiringKey<>(key, lifeTimeMillis);
        ExpiringKey<K> oldKey = expiringKeys.put((K) key, delayedKey);
        if(oldKey != null) {
        	//we should not get at this point
        	//if all our keys are be unique
            expireKey(oldKey);
            expiringKeys.put((K) key, delayedKey);
        }
        delayQueue.offer(delayedKey);
        return internalMap.put(delayedKey, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(Object key) {
        V removedValue = internalMap.remove(new ExpiringKey<>(key));
        expireKey(expiringKeys.remove(key));
        return removedValue;
    }

    /**
     * Not supported.
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
	public boolean renewKey(Object key) {
        ExpiringKey<K> delayedKey = expiringKeys.get(key);
        if (delayedKey != null) {
            delayedKey.renew();
            return true;
        }
        return false;
    }

    private void expireKey(ExpiringKey<K> delayedKey) {
        if (delayedKey != null) {
            delayedKey.expire();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        delayQueue.clear();
        expiringKeys.clear();
        internalMap.clear();
    }

    /**
     * Not supported.
     */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported.
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }
    private class NotifRunner implements Runnable{
    	private NotifRunner(K key) {
			super();
			this.key = key;
		}
		K key;
		@Override
		public void run() {
			setChanged();
            notifyObservers(key);
		}
    }
   
    private void cleanup() throws InterruptedException {
        ExpiringKey<K> delayedKey = delayQueue.take();
        while (delayedKey != null) {
        	internalMap.remove(delayedKey);
            expiringKeys.remove(delayedKey.getKey());
            notifThread.execute(new NotifRunner(delayedKey.getKey()));
            delayedKey = delayQueue.take();
        }
    }
    /**
     * Remove all expired entries. This method should ideally be only invoked on a 
     * startup, as there can be concurrency issues otherwise.
     */
    protected synchronized void removeAllExpired() {
    	for(Iterator<Entry<ExpiringKey<K>, V>> iter = internalMap.entrySet().iterator();iter.hasNext();) {
    		Entry<ExpiringKey<K>, V> entry = iter.next();
    		ExpiringKey<K> key = entry.getKey();
    		if(key.isDelayed()) {
    			iter.remove();
    			expiringKeys.remove(key.getKey());
    		}
    	}
    }

    public static final class ExpiringKey<K> implements Delayed {

        @Override
		public String toString() {
			return "ExpiringKey [startTime=" + startTime + ", maxLifeTimeMillis=" + maxLifeTimeMillis + ", key=" + key
					+ "]";
		}

		public long getStartTime() {
			return startTime;
		}

		public void setStartTime(long startTime) {
			this.startTime = startTime;
		}

		public long getMaxLifeTimeMillis() {
			return maxLifeTimeMillis;
		}


		private long startTime = System.currentTimeMillis();
        private final long maxLifeTimeMillis;
        private final K key;
        
        private ExpiringKey(K key) {
			this(key, 0);
		}
        
        public ExpiringKey(K key, long maxLifeTimeMillis) {
            this.maxLifeTimeMillis = maxLifeTimeMillis;
            this.key = key;
        }

        public K getKey() {
            return key;
        }

        /**
         * {@inheritDoc}
         */
		@Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ExpiringKey<?> other = (ExpiringKey<?>) obj;
            if (this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
                return false;
            }
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + (this.key != null ? this.key.hashCode() : 0);
            return hash;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
        }

        private boolean isDelayed() {
        	return getDelayMillis() <= 0;
        }
        private long getDelayMillis() {
            return (startTime + maxLifeTimeMillis) - System.currentTimeMillis();
        }

        public void renew() {
            startTime = System.currentTimeMillis();
        }

        public void expire() {
            startTime = Long.MIN_VALUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(Delayed that) {
            return Long.compare(this.getDelayMillis(), ((ExpiringKey<?>) that).getDelayMillis());
        }
    }

    private AtomicBoolean isRunning = new AtomicBoolean();
	@Override
	public void run() {
		if(isRunning.compareAndSet(false, true)) {
			try {
				cleanup();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void addListener(ExpirationListener<K> listener) {
		addObserver(listener);
	}
}
