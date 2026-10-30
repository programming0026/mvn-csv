package com.juniverse.csv;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.mongodb.client.*;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * Importa un CSV a MongoDB con el patrón productor-consumidor: un hilo lee el archivo, agrupa las filas en lotes
 * y varios hilos trabajadores los insertan. Detecta el tipo de cada valor (entero, decimal, booleano, fecha o texto).
 *
 * @author ajuar
 */
public class CsvToMongoBatchV1 {

    private static final Pattern INTEGER = Pattern.compile("-?(0|[1-9]\\d*)");
    private static final Pattern DECIMAL = Pattern.compile("-?(0|[1-9]\\d*)\\.\\d+([eE][+-]?\\d+)?");
    private static final int BATCH_SIZE = 1000;
    private static final int QUEUE_CAPACITY = 20;
    private static final int NUM_WORKERS = Runtime.getRuntime().availableProcessors();
    private static final List<Document> POISON_PILL = Collections.emptyList();
    private static final List<SimpleDateFormat> DATE_FORMATS = Arrays.asList(
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd"),
            new SimpleDateFormat("MM/dd/yyyy"),
            new SimpleDateFormat("dd/MM/yyyy"),
            new SimpleDateFormat("yyyy/MM/dd")
    );

    /**
     * Importa el CSV indicado (por defecto {@code datos.csv}) a la colección {@code mydb.file_csv}.
     * @param args opcional: ruta del CSV
     * @throws Exception si falla la lectura o la conexión
     */
    public static void main(String[] args) throws Exception {
Path csvPath = Paths.get(args.length > 0 ? args[0] : "datos.csv");
// La URI se toma de la variable de entorno MONGO_URI; el valor por defecto es solo para desarrollo local.
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://root:secret@localhost:27017");
        String dbName = "mydb";
        String collectionName = "file_csv";

        BlockingQueue<List<Document>> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Lanzar workers
            CountDownLatch workersDone = startWorkerThreads(queue, collection);

            // Leer CSV y procesar en lotes
            int total = processCsvToQueue(csvPath, queue);

            // Mandar la señal de fin a los workers
            sendPoisonPills(queue);

            // Esperar a que todos los workers terminen
            workersDone.await();
            System.out.println("Importados " + total + " registros a MongoDB (concurrencia: " + NUM_WORKERS + " workers)");
        }
    }

    /**
     * Lanza los hilos worker que insertan los documentos en MongoDB
     * concurrentemente.
     */
    private static CountDownLatch startWorkerThreads(BlockingQueue<List<Document>> queue, MongoCollection<Document> collection) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
        CountDownLatch workersDone = new CountDownLatch(NUM_WORKERS);

        for (int i = 0; i < NUM_WORKERS; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        List<Document> batch = queue.take();
                        if (batch == POISON_PILL) {
                            break;
                        }
                        if (!batch.isEmpty()) {
                            collection.insertMany(batch);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    workersDone.countDown();
                }
            });
        }
        // El Executor se apaga cuando todos los workers terminan
        ExecutorService monitor = Executors.newSingleThreadExecutor();
        monitor.submit(() -> {
            try {
                workersDone.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            executor.shutdown();
        });
        monitor.shutdown(); // sin esto su hilo seguía vivo y el programa nunca terminaba
        return workersDone;
    }

    /**
     * Lee el archivo CSV, convierte las filas a Document y las pone en la cola
     * en lotes.
     *
     * @return Número total de registros procesados.
     */
    private static int processCsvToQueue(Path csvPath, BlockingQueue<List<Document>> queue) throws IOException, CsvValidationException, InterruptedException {
        int total = 0;
        try (
                BufferedReader br = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8); 
                CSVReader csvReader = new CSVReader(br)) {
            String[] headers = csvReader.readNext();
            if (headers == null) {
                throw new RuntimeException("Archivo CSV vacío");
            }

            List<Document> batch = new ArrayList<>(BATCH_SIZE);
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                batch.add(csvRowToDocument(headers, row));
                if (batch.size() >= BATCH_SIZE) {
                    queue.put(new ArrayList<>(batch));
                    total += batch.size();
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                queue.put(new ArrayList<>(batch));
                total += batch.size();
            }
        }
        return total;
    }

    /**
     * Convierte una fila de CSV a un Document de MongoDB, autodetectando tipos.
     */
    private static Document csvRowToDocument(String[] headers, String[] row) {
        Document doc = new Document();
        for (int i = 0; i < headers.length && i < row.length; i++) {
            doc.append(headers[i], parseValue(row[i]));
        }
        return doc;
    }

    /**
     * Intenta convertir el valor del CSV al tipo más apropiado: Integer, Long,
     * Double, Boolean, Date, o String.
     */
    private static Object parseValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        value = value.trim();

        // Solo se convierten números "limpios": sin ceros a la izquierda (códigos como 007 o 0412 siguen siendo texto)
        // y sin los formatos que acepta Double.valueOf (NaN, Infinity, 1f, 2d, hexadecimales...).
        if (INTEGER.matcher(value).matches()) {
            try {
                return Integer.valueOf(value);
            } catch (NumberFormatException ignored) {
            }
            try {
                return Long.valueOf(value);
            } catch (NumberFormatException ignored) {
                return value; // demasiado grande para un long: se conserva como texto
            }
        }
        if (DECIMAL.matcher(value).matches()) {
            return Double.valueOf(value);
        }

        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.valueOf(value);
        }

        for (SimpleDateFormat sdf : DATE_FORMATS) {
            sdf.setLenient(false);
            ParsePosition position = new ParsePosition(0);
            Date date = sdf.parse(value, position);
            // Se exige consumir todo el texto: parse() por sí solo aceptaría "2020-01-15abc"
            if (date != null && position.getIndex() == value.length()) {
                return date;
            }
        }

        return value;
    }

    /**
     * Envía la cantidad necesaria de POISON_PILLs para finalizar todos los
     * workers.
     */
    private static void sendPoisonPills(BlockingQueue<List<Document>> queue) throws InterruptedException {
        for (int i = 0; i < NUM_WORKERS; i++) {
            queue.put(POISON_PILL);
        }
    }
}
