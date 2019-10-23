package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

import scala.collection.concurrent.TrieMap

import java.io.{Serializable => JSerializable}

case class Column(name: String, columnType: ColumnType)

class Registry {
  val databases: TrieMap[String, Database] = TrieMap.empty
  val tables: TrieMap[Long, BTable] = TrieMap()
  val unknownTables: TrieMap[Long, BTable] = TrieMap()

  def getDatabase(name: String): Option[Database] = databases.get(name)

  def getTable(databaseName: String, tableName: String): Option[BTable] = databases.get(databaseName).map(_.getTable(tableName))

  def getTable(tableId: Long): Option[BTable] = {
    println(tables)
    tables.get(tableId).orElse(unknownTables.get(tableId))
  }

  def registryUnknownTable(tableId: Long, array: Array[JSerializable]): BTable = {
    val columns = new Array[Column](array.length)
    columns.indices.foreach(i => columns(i) = Column(s"col_$i", ColumnType.NULL))
    val ukTable = BTable(s"unknown_tb_$tableId", "unknown_db", columns)
    unknownTables.putIfAbsent(tableId, ukTable) match {
      case Some(table) => table
      case None => ukTable
    }
  }

  def getOrRegistry(databaseName: String, tableName: String, tableId: Long)(newTable: => BTable): BTable = {
    databases.get(databaseName).map(_.getTable(tableName)) match {
      case Some(table) => table
      case None =>
        val newDB: Database = new Database(databaseName)
        val newTB: BTable = newTable
        val finalTB = databases.putIfAbsent(databaseName, newDB)
          .getOrElse(newDB)
          .setTable(tableName, newTB)
          .getOrElse(newTB)
        if (finalTB == newTB) {
          tables.put(tableId, finalTB)
        }
        unknownTables.remove(tableId)
        println(tables)
        finalTB
    }
  }

  override def toString: String = {
    s"Registry(${databases.values.mkString("{", ", ", "}")})"
  }
}

class Database(name: String) {
  val tables: TrieMap[String, BTable] = TrieMap.empty

  def setTable(name: String, table: BTable): Option[BTable] = tables.putIfAbsent(name, table)

  def getTable(name: String): BTable = tables(name)

  def getName: String = name

  override def toString: String = {
    s"Database($name: ${tables.values.mkString("{", ", ", "}")})"
  }
}


