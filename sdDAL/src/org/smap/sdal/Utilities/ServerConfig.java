package org.smap.sdal.Utilities;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import jakarta.servlet.http.HttpServletRequest;

public class ServerConfig {

	private static int portOverride = 0;
	private static String hostOverride = null;

	static {
		try {
			Context env = (Context) new InitialContext().lookup("java:comp/env");
			try { portOverride = (Integer) env.lookup("server.port"); } catch (NamingException ignored) {}
			try { hostOverride = (String) env.lookup("server.host"); } catch (NamingException ignored) {}
		} catch (NamingException ignored) {}
	}

	private ServerConfig() {}

	public static String getHost(HttpServletRequest request) {
		return (hostOverride != null && !hostOverride.isEmpty()) ? hostOverride : request.getServerName();
	}

	public static int getPortNumber(HttpServletRequest request) {
		return portOverride != 0 ? portOverride : request.getLocalPort();
	}

	public static String getProtocol(HttpServletRequest request) {
		int port = getPortNumber(request);
		return port == 80 ? "http://" : "https://";
	}
}
