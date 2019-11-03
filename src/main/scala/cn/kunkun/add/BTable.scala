package cn.kunkun.add

import java.io.{Serializable => JSerializable}

import cn.kunkun.add.BStatistics.BStatistic

class BTable(schema: Symbol, tbName: Symbol, columns: Seq[BColumn], statistics: Seq[BStatistic] = Seq.empty) extends Registerable[BSchema, BTable] {

  private val columnSeq: Seq[BColumn] = columns.map(_.registerFrom(this))
  private val statisticSeq: Seq[BStatistic] = statistics.map(_.registerFrom(this))
  private val columnIdx: Map[Symbol, Int] = columnSeq.indices.map(i => (columnSeq(i).getName, i)).toMap

  def tableName: Symbol = tbName

  def schemaName: Symbol = schema

  def getSchema: BSchema = throw new UnsupportedOperationException

  def getRegistry: BSchemaRegistry = getSchema.getRegistry

  def generateRow(values: Array[JSerializable]): BRow = new BRow(this, values)

//  def generateRow(bitSet: BitSet, values: Array[JSerializable]): BRow = ???  //TODO

  def getColumnByIndex(index: Int): BColumn = columnSeq(index)

  def getColumnByName(name: Symbol): BColumn = getColumnByIndex(getIndexByColumn(name))

  def getIndexByColumn(name: Symbol): Int = columnIdx(name)

  def getColumns: Seq[BColumn] = columnSeq

  def getStatistics: Seq[BStatistic] = statisticSeq

  override def toString: String = {
    columnSeq.map(column => column.getName + " " + column.getType).mkString(s"Table[($tableName: (", ", ", s"))]")
  }

  override def registerFrom(t: BSchema): BTable = new BTable(schema, tbName, columns, statistics) {

    override def getSchema: BSchema = t
  }
}

object BTable {
  def of(dbName: Symbol, tbName: Symbol)(columns: BColumn*)(statistics: BStatistic*)(implicit registry: BSchemaRegistry) : BTable = {
    registry.setTableIfAbsentAndGet(new BTable(dbName, tbName, columns, statistics))
  }

  def apply(dbName: Symbol, tbName: Symbol, columns: Seq[BColumn], statistics: Seq[BStatistic] = Seq.empty): BTable = {
    new BTable(dbName, tbName, columns, statistics)
  }
}


