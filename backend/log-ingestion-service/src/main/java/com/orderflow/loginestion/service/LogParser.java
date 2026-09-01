package com.orderflow.loginestion.service;

public class LogParser {

    public ParsedLog parse(
            String timestamp,
            String level,
            String service,
            String message,
            int responseTime) {

        return new ParsedLog(
                normalize(timestamp),
                normalize(level).toUpperCase(),
                normalize(service),
                normalize(message),
                responseTime
        );
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    public static class ParsedLog {

        private final String timestamp;
        private final String level;
        private final String service;
        private final String message;
        private final int responseTime;

        public ParsedLog(
                String timestamp,
                String level,
                String service,
                String message,
                int responseTime) {

            this.timestamp = timestamp;
            this.level = level;
            this.service = service;
            this.message = message;
            this.responseTime = responseTime;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public String getLevel() {
            return level;
        }

        public String getService() {
            return service;
        }

        public String getMessage() {
            return message;
        }

        public int getResponseTime() {
            return responseTime;
        }
    }
}