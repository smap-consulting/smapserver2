package org.smap.sdal.Utilities;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

import jakarta.servlet.http.HttpServletRequest;

/*
 * Limits how many organisations can be registered.
 *
 * Registration is public and each one creates an organisation, a user and a project, and
 * emails the address given, so without a limit it can be used to fill the database and to
 * send email to anyone.  A limit per client address stops one source, and a limit for the
 * whole server bounds an attack spread over many addresses.
 *
 * In memory and per JVM, as for TokenThrottle.
 */
public class RegistrationThrottle {

	private RegistrationThrottle() {
	}

	private static final int PER_CLIENT_PER_HOUR = 3;
	private static final int PER_SERVER_PER_HOUR = 20;
	private static final int MAX_TRACKED_CLIENTS = 10000;

	private static final ConcurrentHashMap<String, Bucket> clients = new ConcurrentHashMap<>();
	private static final Bucket server = Bucket.builder()
			.addLimit(Bandwidth.classic(PER_SERVER_PER_HOUR,
					Refill.greedy(PER_SERVER_PER_HOUR, Duration.ofHours(1))))
			.build();

	/*
	 * True, and the registration counted, if another can be made now.  The client is
	 * checked first so that one source cannot use up the whole server's allowance.
	 *
	 * Requests reach Tomcat over AJP from Apache, so getRemoteAddr is the real client
	 * address.  X-Forwarded-For is set by the caller and is not used.
	 */
	public static boolean tryRegister(HttpServletRequest request) {
		String client = request.getRemoteAddr();
		if(clients.size() > MAX_TRACKED_CLIENTS) {
			clients.clear();
		}
		Bucket bucket = clients.computeIfAbsent(client == null ? "" : client, k ->
			Bucket.builder()
				.addLimit(Bandwidth.classic(PER_CLIENT_PER_HOUR,
						Refill.greedy(PER_CLIENT_PER_HOUR, Duration.ofHours(1))))
				.build());
		if(!bucket.tryConsume(1)) {
			return false;
		}
		return server.tryConsume(1);
	}
}
