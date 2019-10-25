package cn.kunkun.binlog

import java.io.{Serializable => JSerializable}

class BTable(tbName: String, dbName: String, columns: Array[BColumn]) {

  private lazy val inner = BRow(this)_

  private val map: Map[String, Int] = {
    val builder = Map.newBuilder[String, Int]
    for (i <- columns.indices) {
      builder += ((columns(i).name, i))
    }
    builder.result()
  }

  def getRow(values: Array[JSerializable]): BRow = inner(values)

  def getColumnByIndex(index: Int): BColumn = columns(index)

  def getIndexByColumn(name: String): Int = map(name)

  def getColumns: Array[BColumn] = columns

  def tableName: String = tbName

  def databaseName: String = dbName

  override def toString: String = {
    columns.map(column => column.name + " " + column.columnType).mkString(s"Table{$tableName: (", ", ", ")}")
  }
}

object BTable {
  def apply(tbName: String, dbName: String, columns: Array[BColumn]): BTable = {
    new BTable(tbName, dbName, columns)
  }
}
