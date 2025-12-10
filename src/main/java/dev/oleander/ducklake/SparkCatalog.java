package dev.oleander.ducklake;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import dev.oleander.ducklake.credentials.Credentials;
import dev.oleander.ducklake.credentials.LocalCredentials;
import dev.oleander.ducklake.credentials.S3Credentials;
import dev.oleander.ducklake.dao.MetadataDao;
import dev.oleander.ducklake.dao.SchemaDao;
import dev.oleander.ducklake.dao.TableColumnDao;
import dev.oleander.ducklake.dao.TableDao;
import dev.oleander.ducklake.models.Metadata;
import dev.oleander.ducklake.models.Schema;
import dev.oleander.ducklake.models.TableColumn;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkCatalog implements TableCatalog, SupportsNamespaces {
  private static Map<String, DataType> COLUMN_TYPE_MAP = new HashMap<>();
  static {
    COLUMN_TYPE_MAP.put("boolean", DataTypes.BooleanType);
    COLUMN_TYPE_MAP.put("int8", DataTypes.ByteType);
    COLUMN_TYPE_MAP.put("int16", DataTypes.ShortType);
    COLUMN_TYPE_MAP.put("int32", DataTypes.IntegerType);
    COLUMN_TYPE_MAP.put("int64", DataTypes.LongType);
    COLUMN_TYPE_MAP.put("uint8", DataTypes.ShortType);
    COLUMN_TYPE_MAP.put("uint16", DataTypes.IntegerType);
    COLUMN_TYPE_MAP.put("uint32", DataTypes.LongType);
    COLUMN_TYPE_MAP.put("uint64", DataTypes.createDecimalType(20, 0));
    COLUMN_TYPE_MAP.put("float32", DataTypes.FloatType);
    COLUMN_TYPE_MAP.put("float64", DataTypes.DoubleType);
    // TODO: decimal(P, S)
    COLUMN_TYPE_MAP.put("time", DataTypes.StringType);
    COLUMN_TYPE_MAP.put("timetz", DataTypes.StringType);
    COLUMN_TYPE_MAP.put("date", DataTypes.DateType);
    COLUMN_TYPE_MAP.put("timestamp", DataTypes.TimestampType);
    COLUMN_TYPE_MAP.put("timestamptz", DataTypes.TimestampType);
    COLUMN_TYPE_MAP.put("timestamp_s", DataTypes.TimestampType);
    COLUMN_TYPE_MAP.put("timestamp_ms", DataTypes.TimestampType);
    // TODO: timestamp_ns
    // TODO: interval
    COLUMN_TYPE_MAP.put("varchar", DataTypes.StringType);
    // TODO: blob
    // TODO json
    COLUMN_TYPE_MAP.put("uuid", DataTypes.StringType);

    // TODO: struct, list, map, and geometry types
  }

  private Jdbi jdbi;
  private String catalogName;
  private MetadataDao metadataDao;
  private SchemaDao schemaDao;
  private TableDao tableDao;
  private TableColumnDao tableColumnDao;
  private Credentials credentials;

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;

    String pgHost = options.get("postgres-host");
    String pgPort = options.get("postgres-port");
    String pgDatabase = options.get("postgres-database");
    String pgUser = options.get("postgres-user");
    String pgPassword = options.get("postgres-password");
    String storage = options.get("storage");
    String s3AccessKeyId = options.get("s3-access-key-id");
    String s3SecretAccessKey = options.get("s3-secret-access-key");
    String s3SessionToken = options.get("s3-session-token");
    String s3Region = options.get("s3-region");

    if (pgHost == null || pgPort == null || pgDatabase == null || pgUser == null) {
      throw new IllegalArgumentException("Postgres connection parameters are missing");
    }

    if (pgPassword == null) {
      pgPassword = "";
    }
    jdbi = Jdbi.create(String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDatabase),
        pgUser, pgPassword).installPlugin(new SqlObjectPlugin());
    schemaDao = jdbi.onDemand(SchemaDao.class);
    tableDao = jdbi.onDemand(TableDao.class);
    tableColumnDao = jdbi.onDemand(TableColumnDao.class);
    metadataDao = jdbi.onDemand(MetadataDao.class);

    if (storage == null) {
      throw new IllegalArgumentException("Storage type is missing");
    }

    if (storage.equals("s3")) {
      this.credentials =
          new S3Credentials(s3AccessKeyId, s3SecretAccessKey, s3SessionToken, s3Region);
    } else if (storage.equals("local")) {
      this.credentials = new LocalCredentials();
    } else {
      throw new IllegalArgumentException("Unsupported storage type: " + storage);
    }
  }

  @Override
  public String[] defaultNamespace() {
    return listNamespaces()[0];
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return schemaDao.getSchema(String.join(".", namespace)).isPresent();
  }

  @Override
  public String[][] listNamespaces() {
    List<Schema> schemas = schemaDao.listSchema();
    return schemas.stream().map(schema -> new String[] {schema.getSchemaName()})
        .toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
    return new String[0][];
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
    return Collections.emptyMap();
  }

  @Override
  public void createNamespace(String[] strings, Map<String, String> map)
      throws NamespaceAlreadyExistsException {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public void alterNamespace(String[] strings, NamespaceChange... namespaceChanges)
      throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public boolean dropNamespace(String[] strings, boolean b)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }

    List<dev.oleander.ducklake.models.Table> tables =
        tableDao.listTable(String.join(".", namespace));
    return tables.stream().map(t -> Identifier.of(namespace, t.getTableName()))
        .toArray(Identifier[]::new);
  }

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    List<Metadata> metadata = metadataDao.listMetadata();
    Optional<Metadata> dataPathOpt =
        metadata.stream().filter(m -> m.getKey().equals("data_path")).findFirst();
    if (!dataPathOpt.isPresent()) {
      throw new IllegalStateException("data_path metadata is missing");
    }
    String dataPath = dataPathOpt.get().getValue();

    Optional<Schema> schemaOpt = schemaDao.getSchema(String.join(".", identifier.namespace()));
    if (!schemaOpt.isPresent()) {
      throw new NoSuchTableException(identifier);
    }
    Schema schema = schemaOpt.get();
    String schemaPath = schema.isPathIsRelative() ? dataPath + schema.getPath() : schema.getPath();

    Optional<dev.oleander.ducklake.models.Table> tableOpt =
        tableDao.getTable(schema.getSchemaName(), identifier.name());
    if (!tableOpt.isPresent()) {
      throw new NoSuchTableException(identifier);
    }
    dev.oleander.ducklake.models.Table table = tableOpt.get();
    String tablePath = table.isPathIsRelative() ? schemaPath + table.getPath() : table.getPath();

    StructType columns = new StructType();
    List<TableColumn> tableColumns =
        tableColumnDao.listColumns(schema.getSchemaName(), table.getTableName());
    for (TableColumn tableColumn : tableColumns) {
      columns = columns.add(tableColumn.getColumnName(),
          COLUMN_TYPE_MAP.get(tableColumn.getColumnType()));
    }

    Transform[] partitioning = new Transform[0];

    return new DucklakeTable(jdbi, catalogName, identifier, table.getTableId(), columns, tablePath,
        partitioning, credentials);
  }

  @Override
  public Table createTable(Identifier identifier, StructType structType, Transform[] transforms,
      Map<String, String> map) throws TableAlreadyExistsException, NoSuchNamespaceException {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... tableChanges)
      throws NoSuchTableException {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public void renameTable(Identifier identifier, Identifier identifier1)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Unsupported operation");
  }
}
