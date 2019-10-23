package cn.kunkun.binlog

import java.io.{Serializable => JSerializable}

class BTable(tbName: String, dbName: String, columns: Array[Column]) {

  def newRow(values: Array[JSerializable]): BRow = {
    val row = BRow(this)
    row.set(values)
    row
  }

  private lazy val inner = BRow(this)
  def reuseRow(values: Array[JSerializable]): BRow = {
    inner.set(values)
    inner
  }

  def getColumns: Array[Column] = columns

  def tableName: String = tbName

  def databaseName: String = dbName

  override def toString: String = {
    s"Table($dbName.$tbName: ${columns.map(column => column.name + " " + column.columnType).mkString("(", ", ", ")")}"
  }
}

object BTable {
  def apply(tbName: String, dbName: String, columns: Array[Column]): BTable = {
    new BTable(tbName, dbName, columns)
  }
}
