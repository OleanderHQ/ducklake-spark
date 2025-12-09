package dev.oleander.ducklake.dao;

import java.util.List;
import java.util.Optional;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import dev.oleander.ducklake.models.Table;

@RegisterBeanMapper(Table.class)
public interface TableDao {
  @SqlQuery("WITH latest_snapshot AS (\n" + "  SELECT max(snapshot_id) AS snapshot_id\n"
      + "  FROM ducklake_snapshot\n" + ")\n" + "\n" + "SELECT\n"
      + "  t.table_name, t.path, t.path_is_relative\n" + "FROM ducklake_table AS t\n"
      + "JOIN ducklake_schema AS s\n" + "  ON t.schema_id = s.schema_id\n"
      + "CROSS JOIN latest_snapshot AS l\n" + "WHERE l.snapshot_id >= t.begin_snapshot\n"
      + "  AND (l.snapshot_id < t.end_snapshot OR t.end_snapshot IS NULL)\n"
      + "  AND s.schema_name = :schemaName")
  List<Table> listTable(@Bind("schemaName") String schemaName);

  @SqlQuery("WITH latest_snapshot AS (\n" + "  SELECT max(snapshot_id) AS snapshot_id\n"
      + "  FROM ducklake_snapshot\n" + ")\n" + "\n" + "SELECT\n"
      + "  t.table_id, t.table_name, t.path, t.path_is_relative\n" + "FROM ducklake_table AS t\n"
      + "JOIN ducklake_schema AS s\n" + "  ON t.schema_id = s.schema_id\n"
      + "CROSS JOIN latest_snapshot AS l\n" + "WHERE l.snapshot_id >= t.begin_snapshot\n"
      + "  AND (l.snapshot_id < t.end_snapshot OR t.end_snapshot IS NULL)\n"
      + "  AND s.schema_name = :schemaName\n" + "  AND t.table_name = :tableName\n" + "LIMIT 1")
  Optional<Table> getTable(@Bind("schemaName") String schemaName,
      @Bind("tableName") String tableName);
}
