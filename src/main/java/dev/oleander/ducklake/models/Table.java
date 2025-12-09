package dev.oleander.ducklake.models;

import org.jdbi.v3.core.mapper.reflect.ColumnName;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Table {
  @ColumnName("tableId")
  private Long tableId;

  @ColumnName("tableName")
  private String tableName;

  @ColumnName("path")
  private String path;

  @ColumnName("path_is_relative")
  private boolean pathIsRelative;
}
