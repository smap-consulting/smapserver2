package org.smap.sdal.Utilities;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.http.HttpServletRequest;

public class ServerConfig {

    private static String host;
    private static int portNumber;

    static {
        try {
            Context env = (Context) new InitialContext().lookup("java:comp/env");

            // Lookup for the environment variables defined in context.xml/server.xml
            // host = (String) env.lookup("server.host");
            portNumber = (Integer) env.lookup("server.port");

        } catch (NamingException e) {
            e.printStackTrace();
        }
    }

    public static String getHost(HttpServletRequest request) {
        if (host != null && !host.isEmpty()) {
            return host;
        }
        return request.getServerName();
    }

    public static int getPortNumber(HttpServletRequest request) {
        if (portNumber != 0) {
            return portNumber;
        }
        return request.getLocalPort();
    }

}
