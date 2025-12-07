package org.smap.mcp;

import com.google.gson.Gson;

public class MCPProcessor {

    private Gson gson = new Gson();

    public String process(String jsonQuery) {
        try {
            MCPQuery query = gson.fromJson(jsonQuery, MCPQuery.class);

            // Dummy processing logic
            String tool = query.getTool();
            Object result;

            if ("get_time".equals(tool)) {
                result = System.currentTimeMillis();
            } else {
                result = "Unknown tool: " + tool;
            }

            MCPResponse response = new MCPResponse("success", result);
            return gson.toJson(response);
        } catch (Exception e) {
            MCPResponse response = new MCPResponse("error", e.getMessage());
            return gson.toJson(response);
        }
    }
}
