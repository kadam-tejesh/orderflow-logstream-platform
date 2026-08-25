package com.orderflow.search_indexing_service.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Data
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LogEntryRequest {

    @NotBlank
    private String level;

    @NotBlank
    private String service;

    @NotNull
    private Long timestamp; // epoch millis

    @NotBlank
    private String message;

    @NotNull
    private Integer responseTime; // millis
}
