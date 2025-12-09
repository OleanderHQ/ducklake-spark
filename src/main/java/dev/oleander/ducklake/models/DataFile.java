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
public class DataFile {
  @ColumnName("data_file_path")
  private String dataFilePath;

  @ColumnName("data_file_path_is_relative")
  private boolean dataFilePathIsRelative;

  @ColumnName("delete_file_path")
  private String deleteFilePath;

  @ColumnName("delete_file_path_is_relative")
  private boolean deleteFilePathIsRelative;
}
