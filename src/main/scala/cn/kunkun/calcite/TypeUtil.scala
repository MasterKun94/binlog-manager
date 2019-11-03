package cn.kunkun.calcite

import cn.kunkun.add.BColumn.Type
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType._
import org.apache.calcite.sql.`type`.SqlTypeName

object TypeUtil {
  val relTypes: Map[ColumnType, SqlTypeName] = Map(
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

  val bColTypes: Map[ColumnType, Type.ColumnType] = Map(
    DECIMAL -> Type.DECIMAL,
    TINY -> Type.TINYINT,
    SHORT -> Type.INTEGER,
    LONG -> Type.BIGINT,
    FLOAT -> Type.FLOAT,
    DOUBLE -> Type.DOUBLE,
    NULL -> Type.NULL,
    TIMESTAMP -> Type.TIMESTAMP,
    LONGLONG -> Type.BIGINT,
    INT24 -> Type.BIGINT,
    DATE -> Type.TIMESTAMP,
    TIME -> Type.TIMESTAMP,
    DATETIME -> Type.TIMESTAMP,
    YEAR -> Type.TIMESTAMP,
    NEWDATE -> Type.DATE,
    VARCHAR -> Type.VARCHAR,
    BIT -> Type.BOOLEAN,

    TIMESTAMP_V2 -> Type.TIMESTAMP,
    DATETIME_V2 -> Type.TIMESTAMP,
    TIME_V2 -> Type.TIMESTAMP,
    JSON -> Type.VARCHAR,
    NEWDECIMAL -> Type.DECIMAL,
    ENUM -> Type.VARCHAR,
    SET -> Type.VARCHAR,
    TINY_BLOB -> Type.VARCHAR,
    MEDIUM_BLOB -> Type.VARCHAR,
    LONG_BLOB -> Type.VARCHAR,
    BLOB -> Type.VARCHAR,
    VAR_STRING -> Type.VARCHAR,
    STRING -> Type.VARCHAR,
    GEOMETRY -> Type.VARCHAR
  )

  val bCol2RelMap: Map[Type.ColumnType, SqlTypeName] = Map(
    Type.TINYINT -> SqlTypeName.TINYINT,
    Type.SMALLINT -> SqlTypeName.SMALLINT,
    Type.SHORT -> SqlTypeName.SMALLINT,
    Type.INT -> SqlTypeName.INTEGER,
    Type.INTEGER -> SqlTypeName.INTEGER,
    Type.BIGINT -> SqlTypeName.BIGINT,
    Type.LONG -> SqlTypeName.BIGINT,
    Type.VARCHAR -> SqlTypeName.VARCHAR,
    Type.STRING -> SqlTypeName.VARCHAR,
    Type.BLOB -> SqlTypeName.VARCHAR,
    Type.TIMESTAMP -> SqlTypeName.TIMESTAMP,
    Type.DATE -> SqlTypeName.DATE,
    Type.BOOLEAN -> SqlTypeName.BOOLEAN,
    Type.FLOAT -> SqlTypeName.FLOAT,
    Type.DOUBLE -> SqlTypeName.DOUBLE,
    Type.DECIMAL -> SqlTypeName.DECIMAL,
    Type.NULL -> SqlTypeName.NULL,
    Type.ANY -> SqlTypeName.ANY
  )

  def getRelDataType(columnType: Type.ColumnType): SqlTypeName = {
    bCol2RelMap(columnType)
  }

  def getBColType(columnType: ColumnType): Type.ColumnType = {
    bColTypes(columnType)
  }

}
