package dev.oleander.ducklake;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jdbi.v3.core.Jdbi;

import dev.oleander.ducklake.s3.Credentials;

public class DucklakeTable extends Table implements SupportsRead {
  private final Jdbi jdbi;
  private final Identifier identifier;
  private final long tableId;
  private final StructType schema;
  private final String path;
  private final Transform[] partitioning;
  private final Credentials credentials;

  public DucklakeTable(Jdbi jdbi, String catalog, Identifier identifier, long tableId,
      StructType schema, String path, Transform[] partitioning, Credentials credentials) {
    super(identifier.name(), catalog, identifier.namespace(), "", "", false);
    this.jdbi = jdbi;
    this.identifier = identifier;
    this.tableId = tableId;
    this.schema = schema;
    this.path = path;
    this.partitioning = partitioning;
    this.credentials = credentials;
  }

  @Override
  public String name() {
    String namespace = String.join(".", identifier.namespace());
    return namespace.isEmpty() ? identifier.name() : namespace + "." + identifier.name();
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Transform[] partitioning() {
    return partitioning;
  }

  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.BATCH_READ);
    return capabilities;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new DucklakeScanBuilder(jdbi, schema, tableId, path, credentials);
  }
}
