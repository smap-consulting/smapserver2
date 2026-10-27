package org.smap.sdal.Utilities;

import java.util.concurrent.ConcurrentHashMap;

/*
 * Limits how often something can be asked for, by key, such as a public request that sends an
 * email to the address entered.  Keyed on what was entered rather than on whether it matched
 * anything, so being told to wait does not reveal that it did.
 *
 * Held in memory, so the limit starts again when the server restarts.
 */
public class RequestLimiter {

	private final ConcurrentHashMap<String, Long> lastRequest = new ConcurrentHashMap<>();
	private final long intervalMs;

	public RequestLimiter(long intervalMs) {
		this.intervalMs = intervalMs;
	}

	/*
	 * Minutes until the key can be asked for again, 0 if it can be now, in which case this
	 * request is recorded
	 */
	public int minutesToWait(String key) {
		long now = System.currentTimeMillis();
		key = key.trim().toLowerCase();

		// Forget requests that no longer limit anything, so the map stays small
		if(lastRequest.size() > 1000) {
			lastRequest.values().removeIf(t -> now - t >= intervalMs);
		}

		Long previous = lastRequest.get(key);
		if(previous != null && now - previous < intervalMs) {
			return (int) Math.max(1, (intervalMs - (now - previous) + 59999) / 60000);
		}
		lastRequest.put(key, now);
		return 0;
	}
}
