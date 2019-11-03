package cn.kunkun.add

import cn.kunkun.add.BColumn.Type.ColumnType
import cn.kunkun.add.BStatistics.BStatistic

class BColumn(name: Symbol, columnType: ColumnType) extends Registerable[BTable, BColumn] {

  def getName: Symbol = name

  def getType: ColumnType = columnType

  def getStatistics: Seq[BStatistic] = unSupport

  def getIndex: Int = throw new UnsupportedOperationException

  override def toString: String = s"Column($name $columnType)"

  override def registerFrom(table: BTable): BColumn = new BColumn(name, columnType) {
    override def getStatistics: Seq[BStatistic] = {
      val idx = getIndex
      table.getStatistics.filter(statistic => statistic.getColIndices.contains(idx))
    }

    override def getIndex: Int = table.getIndexByColumn(name)

    override def toString: String = s"Column($name $columnType ${getStatistics.map(_.name).mkString(",")})"
  }
}

object BColumn {
  def apply(name: Symbol, columnType: ColumnType): BColumn = new BColumn(name, columnType)

  object Type extends Enumeration {
    type ColumnType = Value

    val TINYINT,
    SMALLINT,
    SHORT ,
    INT,
    INTEGER,
    BIGINT,
    LONG,
    VARCHAR,
    STRING,
    BLOB,
    TIMESTAMP,
    DATE,
    BOOLEAN,
    FLOAT,
    DOUBLE,
    DECIMAL,
    NULL,
    ANY = Value
  }

}
