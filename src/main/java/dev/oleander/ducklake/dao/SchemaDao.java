package dev.oleander.ducklake.dao;

import java.util.List;
import java.util.Optional;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import dev.oleander.ducklake.models.Schema;

@RegisterBeanMapper(Schema.class)
public interface SchemaDao {
  @SqlQuery("WITH latest_snapshot AS (\n" + "  SELECT max(snapshot_id) AS snapshot_id\n"
      + "  FROM ducklake_snapshot\n" + ")\n" + "\n" + "SELECT\n" + "  schema_name,\n" + "  path,\n"
      + "  path_is_relative\n" + "FROM ducklake_schema AS s\n" + "CROSS JOIN latest_snapshot AS l\n"
      + "WHERE l.snapshot_id >= s.begin_snapshot\n"
      + "  AND (l.snapshot_id < s.end_snapshot OR s.end_snapshot IS NULL)\n")
  List<Schema> listSchema();

  @SqlQuery("WITH latest_snapshot AS (\n" + "  SELECT max(snapshot_id) AS snapshot_id\n"
      + "  FROM ducklake_snapshot\n" + ")\n" + "\n" + "SELECT\n" + "  schema_name,\n" + "  path,\n"
      + "  path_is_relative\n" + "FROM ducklake_schema AS s\n" + "CROSS JOIN latest_snapshot AS l\n"
      + "WHERE l.snapshot_id >= s.begin_snapshot\n"
      + "  AND (l.snapshot_id < s.end_snapshot OR s.end_snapshot IS NULL)\n"
      + "  AND schema_name = :schemaName\n" + "LIMIT 1")
  Optional<Schema> getSchema(@Bind("schemaName") String schemaName);
}
