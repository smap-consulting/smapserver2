package org.smap.mcp;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class MCPServer {

    private final int port;
    private final MCPProcessor processor;

    public MCPServer(int port, MCPProcessor processor) {
        this.port = port;
        this.processor = processor;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/mcp", new MCPHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
        System.out.println("MCP Server is listening on port " + port);
    }

    class MCPHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equals(exchange.getRequestMethod())) {
                InputStream is = exchange.getRequestBody();
                String query = new String(is.readAllBytes(), StandardCharsets.UTF_8);

                String response = processor.process(query);

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }
}
