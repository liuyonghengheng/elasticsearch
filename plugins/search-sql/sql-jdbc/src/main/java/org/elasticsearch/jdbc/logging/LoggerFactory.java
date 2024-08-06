


package org.elasticsearch.jdbc.logging;

import java.io.IOException;
import java.io.PrintWriter;

public class LoggerFactory {

    public static Logger getLogger(String filePath, LogLevel logLevel) {
        return getLogger(filePath, logLevel, StandardLayout.INSTANCE);
    }

    public static Logger getLogger(String filePath, LogLevel logLevel, Layout layout) {
        try {
            return new FilePrintWriterLogger(filePath, logLevel, layout);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Logger getLogger(PrintWriter printWriter, LogLevel logLevel) {
        return getLogger(printWriter, logLevel, StandardLayout.INSTANCE);
    }

    public static Logger getLogger(PrintWriter printWriter, LogLevel logLevel, Layout layout) {
        return new PrintWriterLogger(printWriter, logLevel, layout);
    }
}
