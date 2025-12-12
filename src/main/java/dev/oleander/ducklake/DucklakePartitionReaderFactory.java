package dev.oleander.ducklake;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import dev.oleander.ducklake.credentials.Credentials;

public class DucklakePartitionReaderFactory implements PartitionReaderFactory {
  private final StructType schema;
  private final Credentials credentials;
  private final Map<String, String> serializedConf;

  public DucklakePartitionReaderFactory(StructType schema, Credentials credentials,
      Map<String, String> serializedConf) {
    this.schema = schema;
    this.credentials = credentials;
    this.serializedConf = serializedConf;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    DucklakeInputPartition p = (DucklakeInputPartition) partition;
    Configuration conf = new Configuration();
    serializedConf.forEach(conf::set);
    return new DucklakePartitionReader(p.getDataFilePath(), p.getDeleteFilePath(), schema,
        credentials, conf);
  }
}
