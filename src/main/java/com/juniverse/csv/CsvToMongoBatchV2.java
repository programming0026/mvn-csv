package com.juniverse.csv;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.bson.Document;

import java.util.logging.Logger;
/**
 * Importa un CSV a MongoDB en lotes de 100 filas con inserciones asíncronas (CompletableFuture).
 * Todos los valores se guardan como texto.
 *
 * @author ajuar
 */
public class CsvToMongoBatchV2 {
    private static final Logger LOGGER = AppLogger.get(CsvToMongoBatchV2.class);


    private static final int BATCH_SIZE = 100;
    private static final int MAX_PARALLEL_INSERTS = 4;

    /**
     * Importa el CSV indicado (por defecto {@code datos.csv}) a la colección {@code mydb.file_csv}.
     * @param args opcional: ruta del CSV
     * @throws CsvValidationException si el CSV está mal formado
     */
    public static void main(String[] args) throws CsvValidationException {
Path csvPath = Paths.get(args.length > 0 ? args[0] : "datos.csv");
// La URI se toma de la variable de entorno MONGO_URI; el valor por defecto es solo para desarrollo local.
        String mongoUri = System.getenv().getOrDefault("MONGO_URI", "mongodb://root:secret@localhost:27017");
        String dbName = "mydb";
        String collectionName = "file_csv";

        ExecutorService executor = Executors.newFixedThreadPool(MAX_PARALLEL_INSERTS);
        List<CompletableFuture<Void>> insertFutures = new ArrayList<>();

        try (MongoClient mongoClient = MongoClients.create(mongoUri); 
                BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8); 
                CSVReader csvReader = new CSVReader(reader)) {

            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            String[] headers = csvReader.readNext();
            if (headers == null) {
                throw new RuntimeException("Archivo CSV vacío");
            }

            List<Document> batch = new ArrayList<>(BATCH_SIZE);
            long totalCount = 0;
            String[] row;
            while ((row = csvReader.readNext()) != null) {
                Document doc = new Document();
                for (int i = 0; i < headers.length && i < row.length; i++) {
                    doc.append(headers[i], row[i]);
                }

                batch.add(doc);

                if (batch.size() == BATCH_SIZE) {
                    List<Document> toInsert = new ArrayList<>(batch);
                    insertFutures.add(
                       CompletableFuture.runAsync(() -> insertBatch(collection, toInsert), executor)
                    );
                    batch.clear();
                    totalCount += BATCH_SIZE;
                    LOGGER.info("Procesadas: " + totalCount);
                }
            }

            if (!batch.isEmpty()) {
                List<Document> toInsert = new ArrayList<>(batch);
                insertFutures.add(
                        CompletableFuture.runAsync(() -> insertBatch(collection, toInsert), executor)
                );
                totalCount += batch.size();
                LOGGER.info("Procesadas: " + totalCount);
            }

            // Esperar a que terminen todas las inserciones
            CompletableFuture.allOf(insertFutures.toArray(CompletableFuture[]::new)).join();
            LOGGER.info("Importacion completada.");

        } catch (IOException e) {
            LOGGER.severe("Error al leer archivo: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Inserta un lote sin orden, de modo que un documento con error no detiene al resto.
     * @param collection colección de destino
     * @param batch documentos a insertar
     */
    private static void insertBatch(MongoCollection<Document> collection, List<Document> batch) {
        try {
            collection.insertMany(batch, new InsertManyOptions().ordered(false));
        } catch (Exception e) {
            LOGGER.severe("Error en inserción batch: " + e.getMessage());
        }
    }

}
