package dev.oleander.ducklake.dao;

import java.util.List;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import dev.oleander.ducklake.models.TableColumn;

@RegisterBeanMapper(TableColumn.class)
public interface TableColumnDao {
  @SqlQuery("WITH latest_snapshot AS (\n" + "  SELECT max(snapshot_id) AS snapshot_id\n"
      + "  FROM ducklake_snapshot\n" + ")\n" + "\n" + "SELECT\n" + "  c.column_name,\n"
      + "  c.column_type\n" + "FROM ducklake_table AS t\n" + "JOIN ducklake_schema AS s\n"
      + "  ON t.schema_id = s.schema_id\n" + "JOIN ducklake_column AS c\n"
      + "  ON t.table_id = c.table_id\n" + "CROSS JOIN latest_snapshot l\n"
      + "WHERE l.snapshot_id >= t.begin_snapshot\n"
      + "  AND (l.snapshot_id < t.end_snapshot OR t.end_snapshot IS NULL)\n"
      + "  AND c.parent_column IS NULL\n" + "  AND s.schema_name = :schemaName\n"
      + "  AND t.table_name = :tableName")
  List<TableColumn> listColumns(@Bind("schemaName") String schemaName,
      @Bind("tableName") String tableName);
}
