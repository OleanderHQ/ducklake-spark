package dev.oleander.ducklake.dao;

import java.util.List;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import dev.oleander.ducklake.models.DataFile;

@RegisterBeanMapper(DataFile.class)
public interface DataFileDao {
  @SqlQuery("WITH latest_snapshot AS (\n" + "    SELECT max(snapshot_id) AS snapshot_id\n"
      + "    FROM ducklake_snapshot\n" + "),\n" + "data_files AS (\n"
      + "  SELECT table_id, data_file_id, path, path_is_relative, file_order\n"
      + "  FROM ducklake_data_file AS d\n" + "  CROSS JOIN latest_snapshot AS l\n"
      + "  WHERE l.snapshot_id >= d.begin_snapshot\n"
      + "    AND (l.snapshot_id < d.end_snapshot OR d.end_snapshot IS NULL)\n" + "),\n"
      + "delete_files AS (\n" + "  SELECT data_file_id, path, path_is_relative\n"
      + "  FROM ducklake_delete_file AS d\n" + "  CROSS JOIN latest_snapshot AS l\n"
      + "  WHERE l.snapshot_id >= d.begin_snapshot\n"
      + "    AND (l.snapshot_id < d.end_snapshot OR d.end_snapshot IS NULL)\n" + ")\n" + "SELECT\n"
      + "  data.path AS data_file_path,\n"
      + "  data.path_is_relative AS data_file_path_is_relative,\n"
      + "  del.path AS delete_file_path,\n"
      + "  del.path_is_relative AS delete_file_path_is_relative\n" + "FROM data_files AS data\n"
      + "LEFT JOIN delete_files AS del\n" + "  ON data.data_file_id = del.data_file_id\n"
      + "WHERE data.table_id = :tableId\n" + "ORDER BY data.file_order")
  List<DataFile> listDataFiles(@Bind("tableId") long tableId);
}
