package com.orderflow.loginestion.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogParserTest {

    private final LogParser logParser = new LogParser();

    @Test
    void shouldParseAndNormalizeLogFields() {

        LogParser.ParsedLog result = logParser.parse(
                "  1700000000000  ",
                "  info ",
                "  order-service ",
                "  Order created successfully  "
        );

        assertEquals("1700000000000", result.getTimestamp());
        assertEquals("INFO", result.getLevel());
        assertEquals("order-service", result.getService());
        assertEquals("Order created successfully", result.getMessage());
    }

    @Test
    void shouldConvertLowercaseLevelToUppercase() {

        LogParser.ParsedLog result = logParser.parse(
                "1700000000000",
                "error",
                "payment-service",
                "Payment failed"
        );

        assertEquals("ERROR", result.getLevel());
    }

    @Test
    void shouldHandleNullValuesAsEmptyStrings() {

        LogParser.ParsedLog result = logParser.parse(
                null,
                null,
                null,
                null
        );

        assertEquals("", result.getTimestamp());
        assertEquals("", result.getLevel());
        assertEquals("", result.getService());
        assertEquals("", result.getMessage());
    }

    @Test
    void shouldTrimWhitespaceFromAllFields() {

        LogParser.ParsedLog result = logParser.parse(
                "  12345  ",
                "  warn  ",
                "  inventory-service  ",
                "  Stock is low  "
        );

        assertEquals("12345", result.getTimestamp());
        assertEquals("WARN", result.getLevel());
        assertEquals("inventory-service", result.getService());
        assertEquals("Stock is low", result.getMessage());
    }

    @Test
    void shouldPreserveMessageCase() {

        LogParser.ParsedLog result = logParser.parse(
                "12345",
                "debug",
                "order-service",
                "Order ID ABC-123 was processed"
        );

        assertEquals("Order ID ABC-123 was processed", result.getMessage());
    }
}