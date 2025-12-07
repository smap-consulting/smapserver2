package org.smap.mcp;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            MCPProcessor processor = new MCPProcessor();
            MCPServer server = new MCPServer(8080, processor);
            server.start();
        } catch (IOException e) {
            System.err.println("Failed to start server: " + e.getMessage());
        }
    }
}
