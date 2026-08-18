/*****************************************************************************

This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

 ******************************************************************************/

package org.smap.sdal.Utilities;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

import jakarta.servlet.http.HttpServletRequest;

/*
 * Slows down anyone working through token values.
 *
 * Keyed on the client address rather than on the token, because a caller guessing tokens
 * presents a different value every time - a per token bucket would never fill.  Only
 * failures are counted, so a device polling with a valid token is never affected however
 * often it calls.
 *
 * A token is 32 random bytes, so this is not what makes guessing infeasible; it is here to
 * stop the attempt costing the server a query per guess.
 *
 * In memory and per JVM.  That is enough for its purpose and keeps the check off the
 * database.  Apache's own Basic authentication failures are invisible from here and need
 * fail2ban on the Apache log instead.
 */
public class TokenThrottle {

	private TokenThrottle() {
	}

	private static final int FAILURES_PER_MINUTE = 20;

	/*
	 * Bounded so that a spray from many forged addresses cannot grow the map without
	 * limit.  When it fills, the map is emptied rather than allowed to grow - losing the
	 * counts is far better than losing the heap, and an attacker gains at most one extra
	 * burst.
	 */
	private static final int MAX_TRACKED_CLIENTS = 10000;

	private static final ConcurrentHashMap<String, Bucket> store = new ConcurrentHashMap<>();

	/*
	 * True when this client may make another token attempt.
	 */
	public static boolean isPermitted(HttpServletRequest request) {

		String client = clientKey(request);
		if(client == null) {
			return true;
		}

		if(store.size() > MAX_TRACKED_CLIENTS) {
			store.clear();
		}

		Bucket bucket = store.computeIfAbsent(client, k ->
			Bucket.builder()
				.addLimit(Bandwidth.classic(FAILURES_PER_MINUTE,
						Refill.greedy(FAILURES_PER_MINUTE, Duration.ofMinutes(1))))
				.build()
		);

		return bucket.getAvailableTokens() > 0;
	}

	/*
	 * Count one failed attempt against this client
	 */
	public static void failed(HttpServletRequest request) {

		String client = clientKey(request);
		if(client == null) {
			return;
		}

		Bucket bucket = store.get(client);
		if(bucket == null) {
			isPermitted(request);				// Creates the bucket
			bucket = store.get(client);
		}
		if(bucket != null) {
			bucket.tryConsume(1);
		}
	}

	/*
	 * Forget the counts.  For tests and for an administrator who has locked themselves out.
	 */
	public static void reset() {
		store.clear();
	}

	/*
	 * Requests reach Tomcat over AJP from Apache on the same host, so getRemoteAddr is
	 * the real client address rather than the proxy.  X-Forwarded-For is not consulted -
	 * it is set by the caller and would let anyone reset their own count.
	 */
	private static String clientKey(HttpServletRequest request) {
		return request == null ? null : request.getRemoteAddr();
	}
}
