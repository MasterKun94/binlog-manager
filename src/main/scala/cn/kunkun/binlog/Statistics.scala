package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType


object Statistics {

  trait Statistic {}

  case class UniqueKey(columns: String*) extends Statistic {
    override def toString: String = columns.mkString("UniqueKey(", ", ", ")")
  }

  case class PrimaryKey(columns: String*) extends Statistic {
    override def toString: String = columns.mkString("PrimaryKey(", ", ", ")")
  }

  case class ForeignKey(foreignTable: BTable, columnPairs: (String, String)*) extends Statistic {
    override def toString: String = columnPairs.mkString(s"ForeignKey[table: ${foreignTable.tableName}](", ", ", ")")
  }

  case class Nullable(columns: String*) extends Statistic {
    override def toString: String = columns.mkString("Nullable(", ", ", ")")
  }

  case class NotNull(columns: String*) extends Statistic {
    override def toString: String = columns.mkString("NotNull(", ", ", ")")
  }

  case class DefaultValue(value: Any, column: String) extends Statistic {
    override def toString: String = s"DefaultValue(value: $value, column: $column)"
  }

  case class SortIndex(ASC: Boolean, columns: String*) extends Statistic {
    override def toString: String = columns.mkString("SortIndex(", ", ", ")")
  }

  def main(args: Array[String]): Unit = {
    val table = BTable("test", "user")(
      BColumn("id", ColumnType.LONG))(
      PrimaryKey("id"))
    println(table)
  }
}
