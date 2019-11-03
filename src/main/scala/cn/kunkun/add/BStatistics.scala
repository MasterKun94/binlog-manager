package cn.kunkun.add

import cn.kunkun.add.BColumn.Type
import cn.kunkun.add.ImplicitUtil.{str2Symbol, _}
import cn.kunkun.add.IndexType.IndexType

import scala.collection.immutable.Stream.cons

object BStatistics {

  abstract class BStatistic(columns: Seq[Symbol]) extends Registerable[BTable, BStatistic] {

    def name: String

    def getColNames: Seq[Symbol] = columns

    def getColumns: Seq[BColumn] = getColIndices.map(getTable.getColumns)

    def getColIndices: Seq[Int] = getColNames.map(getTable.getIndexByColumn)

    def getTable: BTable = unSupport

    def registerFrom(table: BTable): BStatistic = unSupport

    override def toString: String = getColNames.mkString(s"$name[", ", ", "]")
  }

  class None extends BStatistic(Seq.empty) {
    override def name: String = "None"
  }

  class UniqueKey(columns: Seq[Symbol]) extends BStatistic(columns) {

    override def name: String = "UniqueKey"

    override def registerFrom(table: BTable): BStatistic = new UniqueKey(columns) {

      override def getTable: BTable = table
    }
  }

  class PrimaryKey(columns: Seq[Symbol]) extends BStatistic(columns) {

    override def name: String = "PrimaryKey"

    override def registerFrom(table: BTable): BStatistic = new PrimaryKey(columns) {

      override def getTable: BTable = table
    }

  }

  class ForeignKey(columns: Seq[Symbol], foreignColumns: Seq[Symbol], foreignDB: Symbol, foreignTB: Symbol) extends BStatistic(columns) {

    override def name: String = s"ForeignKey($foreignDB.$foreignTB)"

    def getPairNames: Seq[(Symbol, Symbol)] = columns.zip(foreignColumns)

    def getPair: Seq[(BColumn, BColumn)] = unSupport

    def getPairIndices: Seq[(Int, Int)] = unSupport

    def foreign: BStatistic = unSupport

    override def registerFrom(table: BTable): BStatistic = new ForeignKey(columns, foreignColumns, foreignDB, foreignTB) {
      private val db: Symbol = if (foreignDB == null) table.schemaName else foreignDB

      override def getTable: BTable = table

      override def name: String = s"ForeignKey($db.$foreignTB)"

      def getForeignTable: BTable = {
        table.getRegistry
          .getTable(db, foreignTB)
          .getOrElse(unSupport)
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

      override def toString: String = getPairNames.map(t => s"${t._1}->${t._2}").mkString(s"$name[", ", ", "]")

      override def foreign: BStatistic = new BStatistic(foreignColumns) {

        override def getTable: BTable = getForeignTable

        override def name: String = s"ForeignKey.foreign"
      }
    }
  }

  class Nullable(columns: Seq[Symbol]) extends BStatistic(columns) {

    override def name: String = "Nullable"

    override def registerFrom(table: BTable): BStatistic = new Nullable(columns) {

      override def getTable: BTable = table
    }
  }

  class NotNull(columns: Seq[Symbol]) extends BStatistic(columns) {

    override def name: String = "NotNull"

    override def registerFrom(table: BTable): BStatistic = new NotNull(columns) {

      override def getTable: BTable = table
    }
  }

  class DefaultValue(value: Any, columns: Seq[Symbol]) extends BStatistic(columns) {

    override def name: String = s"DefaultValue($value)"

    override def registerFrom(table: BTable): BStatistic = new DefaultValue(value, columns) {

      override def getTable: BTable = table
    }
  }

  class SortIndex(sortType: IndexType, columns: Seq[Symbol]) extends BStatistic(columns) {

    override def name: String = "SortIndex"

    def getSortType: IndexType.Value = sortType

    override def registerFrom(table: BTable): BStatistic = new SortIndex(sortType, columns) {

      override def getTable: BTable = table
    }
  }

  class Index(columns: Seq[Symbol]) extends BStatistic(columns) {

    override def name: String = "Index"

    override def registerFrom(table: BTable): BStatistic = new Index(columns) {

      override def getTable: BTable = table
    }
  }


  object BStatistic {
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
    def of(sortType: IndexType.Value, columns: Symbol*): SortIndex = new SortIndex(sortType, columns)

    def apply(sortType: IndexType.Value, columns: String*): SortIndex = new SortIndex(sortType, columns.map(Symbol.apply))
  }

  object Index {
    def of(columns: Symbol*): Index = new Index(columns)

    def apply(columns: String*): Index = new Index(columns.map(Symbol.apply))
  }


  def main(args: Array[String]): Unit = {

    implicit val registry: BSchemaRegistry = new BSchemaRegistry("hello")
    import ImplicitUtil._

    val table1 = BTable.of("test", "user")(
      "id"     ->   "LONG",
      "name"   ->   Type.VARCHAR,
      "age"    ->   Type.INT
    )(
      PrimaryKey("id"),
      UniqueKey("name", "age"),
      ForeignKey("class", ("id", "user_id"))
    )

    val table22 = BTable.of("test", "class")(
        "id"      ->  "LONG" ,
      "name"    ->  "VARCHAR",
      "user_id" ->  "LONG"
    )()



    registry.setSchemaIfAbsent(new BSchema("test"))
    registry.setTableIfAbsentAndGet(table1)
    registry.setTableIfAbsentAndGet(table22)
    println(registry)
    println(registry.getSchema('test).get.tableNames)
    val table = registry.getTable('test, 'user).get
    val table2 = registry.getTable('test, 'class).get

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

    for (elem <- table.getColumns) {
      println(elem)
      println(elem.getIndex)
      println(elem.getStatistics)
    }
  }

}
