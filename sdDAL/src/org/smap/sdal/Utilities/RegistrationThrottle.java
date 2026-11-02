package org.smap.sdal.Utilities;

import java.net.InetAddress;
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
 * send email to anyone.  A limit per client address stops one source.
 *
 * Where the server has no challenge a limit for the whole server also bounds an attack
 * spread over many addresses.  A few sources can use that up and stop everyone else
 * registering, so where the server has a challenge, which makes each registration cost
 * the sender, only the per client limit applies.
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
	 * checked first so that one source cannot use up the whole server's allowance, and
	 * is given its registration back if the server limit refuses it.
	 *
	 * Requests reach Tomcat over AJP from Apache, so getRemoteAddr is the real client
	 * address.  X-Forwarded-For is set by the caller and is not used.
	 */
	public static boolean tryRegister(HttpServletRequest request, boolean serverLimit) {
		if(clients.size() > MAX_TRACKED_CLIENTS) {
			// A full bucket holds nothing, a client that has not registered recently starts with one
			clients.values().removeIf(b -> b.getAvailableTokens() >= PER_CLIENT_PER_HOUR);
			if(clients.size() > MAX_TRACKED_CLIENTS) {
				clients.clear();		// Bounds the memory, each client still had to pass the challenge
			}
		}
		Bucket bucket = clients.computeIfAbsent(clientKey(request.getRemoteAddr()), k ->
			Bucket.builder()
				.addLimit(Bandwidth.classic(PER_CLIENT_PER_HOUR,
						Refill.greedy(PER_CLIENT_PER_HOUR, Duration.ofHours(1))))
				.build());
		if(!bucket.tryConsume(1)) {
			return false;
		}
		if(serverLimit && !server.tryConsume(1)) {
			bucket.addTokens(1);
			return false;
		}
		return true;
	}

	/*
	 * An IPv6 client usually has a whole /64 to choose addresses from, so it counts as one
	 */
	static String clientKey(String addr) {
		if(addr == null) {
			return "";
		}
		if(addr.indexOf(':') < 0 || !addr.matches("[0-9a-fA-F:.]+")) {
			return addr;		// Only a literal is looked at, so there is never a name lookup
		}
		try {
			InetAddress ia = InetAddress.getByName(addr);
			byte[] b = ia.getAddress();
			if(b.length == 4) {
				return ia.getHostAddress();		// IPv4 mapped
			} else if(b.length == 16) {
				StringBuilder key = new StringBuilder();
				for(int i = 0; i < 8; i += 2) {
					key.append(Integer.toHexString(((b[i] & 0xff) << 8) | (b[i + 1] & 0xff))).append(':');
				}
				return key.append(":/64").toString();
			}
		} catch (Exception e) {
			// Not an address, use as given
		}
		return addr;
	}
}
