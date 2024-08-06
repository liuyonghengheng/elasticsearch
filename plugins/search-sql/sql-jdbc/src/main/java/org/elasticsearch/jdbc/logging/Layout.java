


package org.elasticsearch.jdbc.logging;

public interface Layout {
    String formatLogEntry(LogLevel severity, String message);
}
