package dev.oleander.ducklake;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import dev.oleander.ducklake.credentials.Credentials;

public class DucklakePartitionReaderFactory implements PartitionReaderFactory {
  private final StructType schema;
  private final Credentials credentials;

  public DucklakePartitionReaderFactory(StructType schema, Credentials credentials) {
    this.schema = schema;
    this.credentials = credentials;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    DucklakeInputPartition p = (DucklakeInputPartition) partition;
    return new DucklakePartitionReader(p.getDataFilePath(), p.getDeleteFilePath(), schema,
        credentials);
  }
}
