package dev.oleander.ducklake;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.jdbi.v3.core.Jdbi;

import dev.oleander.ducklake.credentials.Credentials;
import dev.oleander.ducklake.dao.DataFileDao;
import dev.oleander.ducklake.models.DataFile;

public class DucklakeScan implements Scan, Batch {
  private final Jdbi jdbi;
  private final StructType fullSchema;
  private final StructType requiredSchema;
  private final Filter[] pushedFilters;
  private final long tableId;
  private final String basePath;
  private final Credentials credentials;
  private final Map<String, String> serializedHadoopConf;

  public DucklakeScan(Jdbi jdbi, StructType fullSchema, StructType requiredSchema,
      Filter[] pushedFilters, long tableId, String basePath, Credentials credentials,
      Map<String, String> serializedHadoopConf) {
    this.jdbi = jdbi;
    this.fullSchema = fullSchema;
    this.requiredSchema = requiredSchema;
    this.pushedFilters = pushedFilters;
    this.tableId = tableId;
    this.basePath = basePath;
    this.credentials = credentials;
    this.serializedHadoopConf = serializedHadoopConf;
  }

  @Override
  public StructType readSchema() {
    return requiredSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    DataFileDao dataFileDao = jdbi.onDemand(DataFileDao.class);
    List<DataFile> dataFiles = dataFileDao.listDataFiles(tableId);
    return dataFiles.stream().map(f -> {
      String dataFilePath =
          f.isDataFilePathIsRelative() ? basePath + f.getDataFilePath() : f.getDataFilePath();
      String deleteFilePath =
          f.isDeleteFilePathIsRelative() ? basePath + f.getDeleteFilePath() : f.getDeleteFilePath();
      return new DucklakeInputPartition(dataFilePath, deleteFilePath);
    }).toArray(DucklakeInputPartition[]::new);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new DucklakePartitionReaderFactory(requiredSchema, credentials, serializedHadoopConf);
  }
}
