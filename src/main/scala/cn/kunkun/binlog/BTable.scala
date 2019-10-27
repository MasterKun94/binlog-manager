package cn.kunkun.binlog

import java.io.{Serializable => JSerializable}

import cn.kunkun.binlog.Statistics.Statistic

class BTable(dbName: Symbol, tbName: Symbol, columns: Seq[BColumn], statistics: Seq[Statistic] = Seq.empty, registry: Registry) {
  registry.registryTable(this)

  private lazy val inner = BRow(this)_
  private val statisticSeq = statistics.map(_.registerFrom(this))
  private val columnSeq = columns.map(_.registerFrom(this))

  private val map: Map[Symbol, Int] = {
    val builder = Map.newBuilder[Symbol, Int]
    for (i <- columnSeq.indices) {
      builder += ((columnSeq(i).getName, i))
    }
    builder.result()
  }

  def getRegistry: Registry = registry

  def generateNewRow(values: Array[JSerializable]): BRow = inner(values)

  def getColumnByIndex(index: Int): BColumn = columnSeq(index)

  def getIndexByColumn(name: Symbol): Int = map(name)

  def getColumns: Seq[BColumn] = columnSeq

  def getStatistics: Seq[Statistic] = statisticSeq

  def tableName: Symbol = tbName

  def databaseName: Symbol = dbName

  override def toString: String = {
    columnSeq.map(column => column.getName + " " + column.getType).mkString(s"Table[($tableName: (", ", ", s"))]")
  }
}

object BTable {
  def apply(dbName: Symbol, tbName: Symbol)(columns: BColumn*)(statistics: Statistic*)(implicit registry: Registry = Registry.DEFAULT) : BTable = {
    new BTable(dbName, tbName, columns, statistics, registry)
  }

  def of(dbName: Symbol, tbName: Symbol, columns: Seq[BColumn], statistics: Seq[Statistic] = Seq.empty)(implicit registry: Registry = Registry.DEFAULT): BTable = {
    new BTable(dbName, tbName, columns, statistics, registry)
  }
}
