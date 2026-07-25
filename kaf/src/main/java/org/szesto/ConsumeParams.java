package org.szesto;

import java.time.Duration;

public record ConsumeParams(int maxRecords, Duration maxDuration) {
}
