package com.orderflow.loginestion.service;

public class LogParser {

    public ParsedLog parse(
            String timestamp,
            String level,
            String service,
            String message) {

        return new ParsedLog(
                normalize(timestamp),
                normalize(level).toUpperCase(),
                normalize(service),
                normalize(message)
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

        public ParsedLog(
                String timestamp,
                String level,
                String service,
                String message) {

            this.timestamp = timestamp;
            this.level = level;
            this.service = service;
            this.message = message;
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
    }
}