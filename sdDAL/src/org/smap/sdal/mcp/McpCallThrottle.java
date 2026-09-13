package org.smap.sdal.mcp;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

/*
 * A ceiling on how fast one token may call.
 *
 * TokenThrottle counts failures, which is the right shape for somebody working through token values
 * and no use at all here: a valid token failed nothing and was limited by nothing.  Every call is a
 * query or several - dsar_find runs one per personal data column per form - so an authenticated
 * client could spend the server without ever presenting a bad credential.
 *
 * The realistic cause is not an attacker.  It is an agent that retries, or loops over a list making a
 * call per row, which is a thing agents do by accident and at machine speed.
 *
 * Generous on purpose.  This interface is interactive: somebody is waiting for the answer, so a
 * hundred and twenty calls a minute is already far past what a person can read, and anything hitting
 * this ceiling has stopped being a conversation.  A server that sets an API rate lower than that gets
 * the lower of the two, since it has said what it thinks a client is worth.
 *
 * Keyed on the token rather than the address, so one runaway client cannot exhaust the allowance of
 * everyone else behind the same proxy, and revoking a token drops its bucket with it.
 */
public class McpCallThrottle {

	private McpCallThrottle() {
	}

	public static final int DEFAULT_CALLS_PER_MINUTE = 120;

	/*
	 * What is actually being enforced for this server, which is not always the default.
	 *
	 * Exposed so the refusal can name the real number.  It first reported the default whatever was in
	 * force, which told somebody limited to three calls a minute that they had a hundred and twenty -
	 * the one fact they needed, and it was wrong.
	 */
	public static int effectiveLimit(int serverRatePerMinute) {
		return serverRatePerMinute > 0 && serverRatePerMinute < DEFAULT_CALLS_PER_MINUTE
				? serverRatePerMinute
				: DEFAULT_CALLS_PER_MINUTE;
	}

	/*
	 * Bounded, and emptied rather than grown when full - the same trade TokenThrottle makes.  Losing
	 * the counts costs one burst; losing the heap costs the server.
	 */
	private static final int MAX_TRACKED_TOKENS = 10000;

	/*
	 * The bucket and the limit it was built with.
	 *
	 * Kept together because computeIfAbsent only runs the factory when the key is absent, so a bucket
	 * created at one limit would keep that limit for the life of the JVM - and lowering the server's
	 * API rate would silently do nothing to any token that had already called.  A setting that has no
	 * effect until a restart, while the documentation says otherwise, is worse than not having it.
	 */
	private static class Limited {
		final int limit;
		final Bucket bucket;

		Limited(int limit) {
			this.limit = limit;
			this.bucket = Bucket.builder()
					.addLimit(Bandwidth.classic(limit, Refill.greedy(limit, Duration.ofMinutes(1))))
					.build();
		}
	}

	private static final ConcurrentHashMap<Integer, Limited> store = new ConcurrentHashMap<>();

	/*
	 * True when this token may make another call, and counts it.  Unlike the failure throttle this
	 * consumes on every call, because here it is the successful ones that cost something.
	 */
	public static boolean consume(int tokenId, int serverRatePerMinute) {

		int limit = effectiveLimit(serverRatePerMinute);

		if(store.size() > MAX_TRACKED_TOKENS) {
			store.clear();
		}

		/*
		 * Rebuilt when the configured limit has changed, which also resets the allowance - raising
		 * the limit should not leave somebody serving out a sentence passed under the old one.
		 */
		Limited limited = store.compute(tokenId, (k, existing) ->
				existing != null && existing.limit == limit ? existing : new Limited(limit));

		return limited.bucket.tryConsume(1);
	}

	/*
	 * There is deliberately nothing to call when a token is revoked.  A revoked token never resolves,
	 * so the throttle is never reached for it and the bucket is simply orphaned until the map is
	 * cleared - an unused method for tidying it would assert a guarantee nothing depends on.
	 */
}
