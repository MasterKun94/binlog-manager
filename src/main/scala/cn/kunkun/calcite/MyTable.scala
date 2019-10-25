package cn.kunkun.calcite

import cn.kunkun.binlog.BColumn
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable

import scala.collection.JavaConversions._

class MyTable(bColumns: Array[BColumn]) extends AbstractTable {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    typeFactory.createStructType(bColumns
      .map(col => (col.name, typeFactory.createSqlType(TypeUtil.getRelDataType(col.columnType))))
      .toMap
      .entrySet()
      .toSeq)
  }
}
