package dev.oleander.ducklake;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import dev.oleander.ducklake.credentials.Credentials;
import dev.oleander.ducklake.credentials.S3Credentials;

public class DucklakePartitionReader implements PartitionReader<InternalRow> {
  private final String dataFilePath;
  private final String deleteFilePath;
  private final StructType schema;
  private final Credentials credentials;
  private final Configuration sparkConf;

  private boolean initialized;
  private ParquetReader<Group> dataReader;

  private final Set<Long> deletedPositions = new HashSet<>();
  private long currentPosition = 0L;

  private InternalRow currentRow;

  public DucklakePartitionReader(String dataFilePath, String deleteFilePath, StructType schema,
      Credentials credentials, Configuration sparkConf) {
    this.dataFilePath = dataFilePath;
    this.deleteFilePath = deleteFilePath;
    this.schema = schema;
    this.credentials = credentials;
    this.sparkConf = sparkConf;
  }

  private void initIfNeeded() throws IOException {
    if (initialized) {
      return;
    }

    Configuration conf = new Configuration(sparkConf);

    if (credentials instanceof S3Credentials) {
      S3Credentials s3Credentials = (S3Credentials) credentials;
      conf.set("fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
      conf.set("fs.s3a.access.key", s3Credentials.getAccessKeyId());
      conf.set("fs.s3a.secret.key", s3Credentials.getSecretAccessKey());
      conf.set("fs.s3a.session.token", s3Credentials.getSessionToken());
      conf.set("fs.s3a.endpoint", "s3." + s3Credentials.getRegion() + ".amazonaws.com");
    }

    // If a delete file is provided, load deleted row positions from it.
    if (deleteFilePath != null && !deleteFilePath.isEmpty()) {
      String deletePathStr = deleteFilePath.replaceFirst("^s3://", "s3a://");
      Path deletePath = new Path(deletePathStr);

      GroupReadSupport deleteReadSupport = new GroupReadSupport();
      try (ParquetReader<Group> deleteReader =
          ParquetReader.builder(deleteReadSupport, deletePath).withConf(conf).build()) {
        Group deleteGroup;
        while ((deleteGroup = deleteReader.read()) != null) {
          // delete file schema: file_path (ignored), pos (0-based row index in data file)
          long pos = deleteGroup.getLong("pos", 0);
          deletedPositions.add(pos);
        }
      }
    }

    Path dataPath = new Path(dataFilePath.replaceFirst("^s3://", "s3a://"));

    GroupReadSupport readSupport = new GroupReadSupport();
    dataReader = ParquetReader.builder(readSupport, dataPath).withConf(conf).build();

    initialized = true;
  }

  @Override
  public boolean next() throws IOException {
    initIfNeeded();

    while (true) {
      Group group = dataReader.read();
      if (group == null) {
        return false;
      }

      if (deletedPositions.contains(currentPosition)) {
        currentPosition++;
        continue;
      }

      currentRow = convertGroupToInternalRow(group, schema);
      currentPosition++;
      return true;
    }
  }

  private InternalRow convertGroupToInternalRow(Group g, StructType schema) {
    int n = schema.size();
    Object[] values = new Object[n];

    for (int i = 0; i < n; i++) {
      StructField field = schema.apply(i);
      String colName = field.name();
      DataType dt = field.dataType();

      int rep = g.getFieldRepetitionCount(colName);
      if (rep == 0) {
        values[i] = null;
        continue;
      }

      if (dt.equals(DataTypes.StringType)) {
        values[i] = UTF8String.fromString(g.getString(colName, 0));
      } else if (dt.equals(DataTypes.BooleanType)) {
        values[i] = g.getBoolean(colName, 0);
      } else if (dt.equals(DataTypes.ByteType)) {
        int v = g.getInteger(colName, 0);
        values[i] = (byte) v;
      } else if (dt.equals(DataTypes.ShortType)) {
        int v = g.getInteger(colName, 0);
        values[i] = (short) v;
      } else if (dt.equals(DataTypes.IntegerType)) {
        values[i] = g.getInteger(colName, 0);
      } else if (dt.equals(DataTypes.LongType)) {
        values[i] = g.getLong(colName, 0);
      } else if (dt.equals(DataTypes.FloatType)) {
        values[i] = g.getFloat(colName, 0);
      } else if (dt.equals(DataTypes.DoubleType)) {
        values[i] = g.getDouble(colName, 0);
      } else if (dt instanceof DecimalType) {
        DecimalType decType = (DecimalType) dt;
        long raw = g.getLong(colName, 0);
        values[i] = Decimal.apply(raw, decType.precision(), decType.scale());
      } else if (dt.equals(DataTypes.DateType)) {
        int days = g.getInteger(colName, 0);
        values[i] = days;
      } else if (dt.equals(DataTypes.TimestampType)) {
        long micros = g.getLong(colName, 0);
        values[i] = micros;
      } else {
        // Fallback: unsupported or not yet implemented type mapping
        values[i] = null;
      }
    }

    return new GenericInternalRow(values);
  }

  @Override
  public InternalRow get() {
    return currentRow;
  }

  @Override
  public void close() throws IOException {
    if (dataReader != null) {
      dataReader.close();
      dataReader = null;
    }
  }
}
