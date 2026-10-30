# Importador de CSV a MongoDB

Importa archivos CSV grandes a MongoDB de forma eficiente, leyendo por lotes e insertando en paralelo. Incluye dos variantes.

## Características

- **V1**: productor-consumidor con cola bloqueante y un hilo trabajador por núcleo; detecta tipos (entero, decimal, booleano, fecha, texto).
- **V2** (la que ejecuta Maven por defecto): lotes de 100 filas con `CompletableFuture`; todos los valores como texto.
- Inserción sin orden: un documento con error no detiene al resto del lote.
- Configuración por variable de entorno `MONGO_URI` y ruta del CSV por argumento.

## Requisitos

- JDK 21 y Maven 3.9 o superior.
- Una instancia de MongoDB accesible (por ejemplo `docker run -d -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=secret mongo`).

## Compilar y ejecutar

```bash
export MONGO_URI="mongodb://usuario:clave@localhost:27017"
mvn compile exec:java -Dexec.args="datos.csv"

# Variante V1
mvn compile exec:java -Dexec.mainClass=com.juniverse.csv.CsvToMongoBatchV1 -Dexec.args="datos.csv"
```

## Notas

- La primera fila del CSV debe contener los nombres de las columnas.
- Los datos se guardan en la base `mydb`, colección `file_csv`.
- En V1, los valores con ceros a la izquierda (por ejemplo `007`) se conservan como texto para no perderlos.

## JAR ejecutable (fat jar)

```bash
mvn clean package
java -jar target/mvn-csv-1.0-SNAPSHOT-launcher.jar
```

El archivo `-launcher.jar` incluye todas las dependencias.

## Licencia

Código de ejemplo de uso libre con fines educativos.
