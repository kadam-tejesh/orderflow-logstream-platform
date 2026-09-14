package com.orderflow.search_indexing_service.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class BatchLogEntryRequest {

    @NotEmpty
    @Valid
    private List<LogEntryRequest> logs;

    public List<LogEntryRequest> getLogs() {
        return logs;
    }

    public void setLogs(List<LogEntryRequest> logs) {
        this.logs = logs;
    }
}