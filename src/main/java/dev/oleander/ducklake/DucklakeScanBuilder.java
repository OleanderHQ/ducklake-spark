package dev.oleander.ducklake;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.jdbi.v3.core.Jdbi;

import dev.oleander.ducklake.credentials.Credentials;

public class DucklakeScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {
  private final Jdbi jdbi;
  private final StructType fullSchema;
  private final long tableId;
  private final String basePath;
  private StructType requiredSchema;
  private Filter[] pushedFilters;
  private Credentials credentials;

  public DucklakeScanBuilder(Jdbi jdbi, StructType schema, long tableId, String basePath,
      Credentials credentials) {
    this.jdbi = jdbi;
    this.fullSchema = schema;
    this.requiredSchema = schema;
    this.tableId = tableId;
    this.basePath = basePath;
    this.credentials = credentials;
  }

  @Override
  public Scan build() {
    Map<String, String> serializedConf =
        serializeHadoopConf(SparkSession.active().sparkContext().hadoopConfiguration());
    return new DucklakeScan(jdbi, fullSchema, requiredSchema, pushedFilters, tableId, basePath,
        credentials, serializedConf);
  }

  private Map<String, String> serializeHadoopConf(Configuration conf) {
    Map<String, String> values = new HashMap<>();
    conf.iterator().forEachRemaining(entry -> values.put(entry.getKey(), entry.getValue()));
    return values;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    this.pushedFilters = filters;
    return new Filter[0];
  }

  @Override
  public Filter[] pushedFilters() {
    return new Filter[0];
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }
}
