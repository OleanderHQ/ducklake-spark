package dev.oleander.ducklake.dao;

import java.util.List;

import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import dev.oleander.ducklake.models.Metadata;

@RegisterBeanMapper(Metadata.class)
public interface MetadataDao {
  @SqlQuery("SELECT key, value FROM ducklake_metadata")
  List<Metadata> listMetadata();
}
