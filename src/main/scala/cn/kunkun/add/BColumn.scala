package cn.kunkun.add

import cn.kunkun.add.Statistics.Statistic
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

class BColumn(name: Symbol, columnType: ColumnType) extends Registerable[BTable, BColumn] {

  def getName: Symbol = name

  def getType: ColumnType = columnType

  def getStatistics: Seq[Statistic] = throw new UnsupportedOperationException

  def getIndex: Int = throw new UnsupportedOperationException

  override def toString: String = s"Column($name $columnType)"

  override def registerFrom(table: BTable): BColumn = new BColumn(name, columnType) {
    override def getStatistics: Seq[Statistic] = {
      table.getStatistics.filter(statistic => statistic.getColIndices.contains(getIndex))
    }

    override def getIndex: Int = table.getIndexByColumn(name)

    override def toString: String = s"Column($name $columnType ${getStatistics.map(_.name).mkString(",")})"
  }
}

object BColumn {
  def apply(name: Symbol, columnType: ColumnType): BColumn = new BColumn(name, columnType)
}
