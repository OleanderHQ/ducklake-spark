package dev.oleander.ducklake;

import org.apache.spark.sql.connector.read.InputPartition;

import lombok.Getter;

@Getter
public class DucklakeInputPartition implements InputPartition {
  private final String dataFilePath;
  private final String deleteFilePath;

  public DucklakeInputPartition(String dataFilePath, String deleteFilePath) {
    this.dataFilePath = dataFilePath;
    this.deleteFilePath = deleteFilePath;
  }
}
