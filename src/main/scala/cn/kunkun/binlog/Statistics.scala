package cn.kunkun.binlog

import cn.kunkun.calcite.SortType
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

import ImplicitUtil._
import ImplicitUtil.str2Symbol

object Statistics {

  val unSupport: () => Nothing = () => throw new UnsupportedOperationException

  abstract class Statistic(columns: Seq[Symbol]) extends Registerable[BTable, Statistic] {

    def name: String

    def getColNames: Seq[Symbol] = columns

    def getColumns: Seq[BColumn] = getColIndices.map(getTable.getColumns)

    def getColIndices: Seq[Int] = getColNames.map(getTable.getIndexByColumn)

    def getTable: BTable = unSupport()

    def registerFrom(table: BTable): Statistic = unSupport()

    override def toString: String = getColNames.mkString(s"$name[", ", ", "]")
  }

  class None extends Statistic(Seq.empty) {
    override def name: String = "None"
  }

  class UniqueKey(columns: Seq[Symbol]) extends Statistic(columns) {

    override def name: String = "UniqueKey"

    override def registerFrom(table: BTable): Statistic = new UniqueKey(columns) {

      override def getTable: BTable = table
    }
  }

  class PrimaryKey(columns: Seq[Symbol]) extends Statistic(columns) {

    override def name: String = "PrimaryKey"

    override def registerFrom(table: BTable): Statistic = new PrimaryKey(columns) {

      override def getTable: BTable = table
    }

  }

  class ForeignKey(columns: Seq[Symbol], foreignColumns: Seq[Symbol], foreignDB: Symbol, foreignTB: Symbol) extends Statistic(columns) {

    override def name: String = s"ForeignKey($foreignDB.$foreignTB)"

    def getPairNames: Seq[(Symbol, Symbol)] = columns.zip(foreignColumns)

    def getPair: Seq[(BColumn, BColumn)] = unSupport()

    def getPairIndices: Seq[(Int, Int)] = unSupport()

    def foreign: Statistic = unSupport()

    override def registerFrom(table: BTable): Statistic = new ForeignKey(columns, foreignColumns, foreignDB, foreignTB) {
      private val db = if (foreignDB == null) table.databaseName else foreignDB

      override def getTable: BTable = table

      override def name: String = s"ForeignKey($db.$foreignTB)"

      def getForeignTable: BTable = {
        table.getRegistry
          .getTable(db, foreignTB)
          .getOrElse(unSupport())
      }

      override def getPair: Seq[(BColumn, BColumn)] = {
        val foreignTable: BTable = getForeignTable
        getPairIndices.map(idx => {
          (table.getColumns(idx._1), foreignTable.getColumns(idx._2))
        })
      }

      override def getPairIndices: Seq[(Int, Int)] = {
        val foreignTable: BTable = getForeignTable
        getColIndices.zip(foreignColumns.map(foreignTable.getIndexByColumn))
      }

      override def foreign: Statistic = new Statistic(foreignColumns) {

        override def getTable: BTable = getForeignTable

        override def name: String = s"ForeignKey.foreign"
      }
    }
  }

  class Nullable(columns: Seq[Symbol]) extends Statistic(columns) {

    override def name: String = "Nullable"

    override def registerFrom(table: BTable): Statistic = new Nullable(columns) {

      override def getTable: BTable = table
    }
  }

  class NotNull(columns: Seq[Symbol]) extends Statistic(columns) {

    override def name: String = "NotNull"

    override def registerFrom(table: BTable): Statistic = new NotNull(columns) {

      override def getTable: BTable = table
    }
  }

  class DefaultValue(value: Any, columns: Seq[Symbol]) extends Statistic(columns) {

    override def name: String = s"DefaultValue($value)"

    override def registerFrom(table: BTable): Statistic = new DefaultValue(value, columns) {

      override def getTable: BTable = table
    }
  }

  class SortIndex(sortType: SortType.Value, columns: Seq[Symbol]) extends Statistic(columns) {

    override def name: String = "SortIndex"

    def getSortType: SortType.Value = sortType

    override def registerFrom(table: BTable): Statistic = new SortIndex(sortType, columns) {

      override def getTable: BTable = table
    }
  }

  class Index(columns: Seq[Symbol]) extends Statistic(columns) {

    override def name: String = "Index"

    override def registerFrom(table: BTable): Statistic = new Index(columns) {

      override def getTable: BTable = table
    }
  }


  object Statistic {
    def none: None = new None()
  }

  object UniqueKey {

    def of(columns: Symbol*): UniqueKey = new UniqueKey(columns)

    def apply(columns: String*): UniqueKey = new UniqueKey(columns.map(Symbol.apply))
  }

  object PrimaryKey {
    def of(columns: Symbol*): PrimaryKey = new PrimaryKey(columns)

    def apply(columns: String*): PrimaryKey = new PrimaryKey(columns.map(Symbol.apply))
  }

  object ForeignKey {
    def apply(foreignTable: String, foreignDB: String, columnPairs: (String, String)*): ForeignKey = {
      new ForeignKey(columnPairs.map(_._1).map(Symbol.apply), columnPairs.map(_._2).map(Symbol.apply), foreignDB, foreignTable)
    }

    def of(foreignTable: Symbol, foreignDB: Symbol, columnPairs: (Symbol, Symbol)*): ForeignKey = {
      new ForeignKey(columnPairs.map(_._1), columnPairs.map(_._2), foreignDB, foreignTable)
    }

    def apply(foreignTable: String, columnPairs: (String, String)*): ForeignKey = {
      new ForeignKey(columnPairs.map(_._1).map(Symbol.apply), columnPairs.map(_._2).map(Symbol.apply), null, foreignTable)
    }

    def of(foreignTable: Symbol, columnPairs: (Symbol, Symbol)*): ForeignKey = {
      new ForeignKey(columnPairs.map(_._1), columnPairs.map(_._2), null, foreignTable)
    }
  }

  object Nullable {
    def of(columns: Symbol*): Nullable = new Nullable(columns)

    def apply(columns: String*): Nullable = new Nullable(columns.map(Symbol.apply))
  }

  object NotNull {
    def of(columns: Symbol*): NotNull = new NotNull(columns)

    def apply(columns: String*): NotNull = new NotNull(columns.map(Symbol.apply))
  }

  object DefaultValue {
    def of(value: Any, columns: Symbol*): DefaultValue = new DefaultValue(value, columns)

    def apply(value: Any, columns: String*): DefaultValue = new DefaultValue(value, columns.map(Symbol.apply))
  }

  object SortIndex {
    def of(sortType: SortType.Value, columns: Symbol*): SortIndex = new SortIndex(sortType, columns)

    def apply(sortType: SortType.Value, columns: String*): SortIndex = new SortIndex(sortType, columns.map(Symbol.apply))
  }

  object Index {
    def of(columns: Symbol*): Index = new Index(columns)

    def apply(columns: String*): Index = new Index(columns.map(Symbol.apply))
  }


  def main(args: Array[String]): Unit = {

    implicit val registry: Registry = new Registry("hello")


    val table = BTable("test", "user")(
      "id"     ->   "LONG",
      "name"   ->   ColumnType.VARCHAR,
      "age"    ->   ColumnType.INT24
    )(
      PrimaryKey("id"),
      UniqueKey("name", "age"),
      ForeignKey("class", ("id", "user_id"))
    )

    val table2 = BTable("test", "class")(
      "id"      ->  "LONG" ,
      "name"    ->  "VARCHAR",
      "user_id" ->  "LONG"
    )()

    val a = 'hafa
    println(a.getClass)

    println(table2)

    println(table)
    println(table.getStatistics)
    for (elem <- table.getStatistics) {
      elem match {
        case key: PrimaryKey =>
          println(key)
          println(key.getColIndices)
          println(key.getColumns)
          println(key.getColNames)
        case key: UniqueKey =>
          println(key)
          println(key.getColIndices)
          println(key.getColumns)
          println(key.getColNames)
        case key: ForeignKey =>
          println(key)
          println(key.getColIndices)
          println(key.getColumns)
          println(key.getColNames)
          println(key.foreign)
          println(key.foreign.getTable)
          println(key.foreign.getColIndices)
          println(key.foreign.getColumns)
          println(key.foreign.getColNames)
      }
    }

    for (elem <- table2.getStatistics) {
      elem match {
        case key: ForeignKey =>
          println(key)
          println(key.getColIndices)
          println(key.getColumns)
          println(key.getColNames)
          println(key.foreign)
          println(key.foreign.getTable)
          println(key.foreign.getColIndices)
          println(key.foreign.getColumns)
          println(key.foreign.getColNames)
      }
    }
    println(table2.getRegistry)
    println(table.getRegistry)

    for (elem <- table2.getColumns) {
      println(elem)
      println(elem.getIndex)
      println(elem.getStatistics)
    }

  }
}
