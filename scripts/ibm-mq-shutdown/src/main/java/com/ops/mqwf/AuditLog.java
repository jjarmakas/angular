package com.ops.mqwf;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Writes every action to the console and to a daily append-only audit file, flushing each line
 * so the trail survives a crash or Ctrl+C.
 */
public class AuditLog implements Closeable {
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final PrintWriter file;
    private final String runId;

    private AuditLog(PrintWriter file, String runId) {
        this.file = file;
        this.runId = runId;
    }

    public static AuditLog open(Path dir, String runId) throws IOException {
        Files.createDirectories(dir);
        Path path = dir.resolve("mqwf-audit-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE) + ".log");
        PrintWriter out = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(path,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE),
                StandardCharsets.UTF_8));
        return new AuditLog(out, runId);
    }

    /** Console-only log, used before the configuration is loaded and in tests. */
    public static AuditLog console() {
        return new AuditLog(null, "-");
    }

    public void info(String format, Object... args) {
        write("INFO ", format, args);
    }

    public void warn(String format, Object... args) {
        write("WARN ", format, args);
    }

    public void error(String format, Object... args) {
        write("ERROR", format, args);
    }

    private synchronized void write(String level, String format, Object... args) {
        String msg = args.length == 0 ? format : String.format(Locale.ROOT, format, args);
        String line = LocalDateTime.now().format(TS) + " " + level + " [" + runId + "] " + msg;
        if ("INFO ".equals(level)) {
            System.out.println(line);
        } else {
            System.err.println(line);
        }
        if (file != null) {
            file.println(line);
            file.flush();
        }
    }

    @Override
    public synchronized void close() {
        if (file != null) {
            file.close();
        }
    }
}
