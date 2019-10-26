package cn.kunkun.binlog

import java.io.{Serializable => JSerializable}

import cn.kunkun.binlog.Statistics.Statistic

class BTable(dbName: String, tbName: String, columns: Seq[BColumn], statistics: Seq[Statistic] = Seq.empty) {

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

  def getColumns: Seq[BColumn] = columns

  def getStatistics: Seq[Statistic] = statistics

  def tableName: String = tbName

  def databaseName: String = dbName

  override def toString: String = {
    columns.map(column => column.name + " " + column.columnType).mkString(s"Table[($tableName: (", ", ", s"))]")
  }
}

object BTable {
  def apply(dbName: String, tbName: String)(columns: BColumn*)(statistics: Statistic*) : BTable = {
    new BTable(dbName, tbName, columns, statistics)
  }

  def of(dbName: String, tbName: String, columns: Seq[BColumn], statistics: Seq[Statistic] = Seq.empty): BTable = {
    new BTable(dbName, tbName, columns, statistics)
  }
}
