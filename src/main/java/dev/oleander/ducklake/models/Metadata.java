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
public class Metadata {
  @ColumnName("key")
  private String key;

  @ColumnName("value")
  private String value;
}
