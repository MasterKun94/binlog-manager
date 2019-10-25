package cn.kunkun.calcite

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType._
import org.apache.calcite.sql.`type`.SqlTypeName

object TypeUtil {
  val types: Map[ColumnType, SqlTypeName] = Map(
    DECIMAL -> SqlTypeName.DECIMAL,
    TINY -> SqlTypeName.SMALLINT,
    SHORT -> SqlTypeName.INTEGER,
    LONG -> SqlTypeName.BIGINT,
    FLOAT -> SqlTypeName.FLOAT,
    DOUBLE -> SqlTypeName.DOUBLE,
    NULL -> SqlTypeName.NULL,
    TIMESTAMP -> SqlTypeName.TIMESTAMP,
    LONGLONG -> SqlTypeName.BIGINT,
    INT24 -> SqlTypeName.BIGINT,
    DATE -> SqlTypeName.TIMESTAMP,
    TIME -> SqlTypeName.TIMESTAMP,
    DATETIME -> SqlTypeName.TIMESTAMP,
    YEAR -> SqlTypeName.INTERVAL_YEAR,
    NEWDATE -> SqlTypeName.DATE,
    VARCHAR -> SqlTypeName.VARCHAR,
    BIT -> SqlTypeName.BOOLEAN,

    TIMESTAMP_V2 -> SqlTypeName.TIMESTAMP,
    DATETIME_V2 -> SqlTypeName.TIMESTAMP,
    TIME_V2 -> SqlTypeName.TIMESTAMP,
    JSON -> SqlTypeName.VARCHAR,
    NEWDECIMAL -> SqlTypeName.DECIMAL,
    ENUM -> SqlTypeName.VARCHAR,
    SET -> SqlTypeName.MULTISET,
    TINY_BLOB -> SqlTypeName.VARCHAR,
    MEDIUM_BLOB -> SqlTypeName.VARCHAR,
    LONG_BLOB -> SqlTypeName.VARCHAR,
    BLOB -> SqlTypeName.VARCHAR,
    VAR_STRING -> SqlTypeName.VARCHAR,
    STRING -> SqlTypeName.VARCHAR,
    GEOMETRY -> SqlTypeName.VARCHAR
  )

  def getRelDataType(columnType: ColumnType): SqlTypeName = {
    types(columnType)
  }

}
