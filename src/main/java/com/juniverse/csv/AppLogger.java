package com.juniverse.csv;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 * Fábrica de loggers del proyecto. Carga una sola vez {@code logging.properties} (formato limpio, sin fecha ni clase), envía el registro a la salida estándar en UTF-8
 * y entrega el {@link Logger} de cada clase.
 */
public final class AppLogger {

    private static final String CONFIG_FILE = "/logging.properties";

    static {
        try (InputStream in = AppLogger.class.getResourceAsStream(CONFIG_FILE)) {
            if (in != null) {
                LogManager.getLogManager().readConfiguration(in);
            }
        } catch (IOException e) {
            System.err.println("No se pudo cargar " + CONFIG_FILE + ": " + e.getMessage());
        }
        useUtf8Console();
    }

    /** Sustituye el manejador por defecto (stderr) por uno que escribe en la salida estándar en UTF-8. */
    private static void useUtf8Console() {
        Logger root = Logger.getLogger("");
        for (Handler handler : root.getHandlers()) {
            root.removeHandler(handler);
        }
        PrintStream console = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        Handler handler = new StreamHandler(console, new SimpleFormatter()) {
            @Override
            public synchronized void publish(LogRecord logRecord) {
                super.publish(logRecord);
                flush();
            }
        };
        handler.setLevel(Level.ALL);
        root.addHandler(handler);
    }

    private AppLogger() {
    }

    /**
     * Devuelve el logger de la clase indicada.
     *
     * @param type clase que va a registrar mensajes
     * @return logger configurado con {@code logging.properties}
     */
    public static Logger get(Class<?> type) {
        return Logger.getLogger(type.getName());
    }
}
