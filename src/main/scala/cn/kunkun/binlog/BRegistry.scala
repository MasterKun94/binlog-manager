package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

import scala.collection.concurrent.TrieMap

import java.io.{Serializable => JSerializable}

class Registry {
  private val databases: TrieMap[String, Database] = TrieMap.empty
  private val tables: TrieMap[Long, BTable] = TrieMap()
  private val unknownTables: TrieMap[Long, BTable] = TrieMap()

  def getDatabase(name: String): Option[Database] = databases.get(name)

  def getTable(databaseName: String, tableName: String): Option[BTable] = databases.get(databaseName).map(_.getTable(tableName))

  def getTable(tableId: Long): Option[BTable] = {
    tables.get(tableId).orElse(unknownTables.get(tableId))
  }

  def registryUnknownTable(tableId: Long, array: Array[JSerializable]): BTable = {
    val columns = new Array[BColumn](array.length)
    columns.indices.foreach(i => columns(i) = BColumn(s"col_$i", ColumnType.NULL))
    val ukTable = BTable(s"unknown_tb_$tableId", "unknown_db", columns)
    unknownTables.putIfAbsent(tableId, ukTable) match {
      case Some(table) => table
      case None => ukTable
    }
  }

  def getOrRegistry(databaseName: String, tableName: String, tableId: Long)(newTable: => BTable): BTable = {
    tables.get(tableId) match {
      case Some(table) => table
      case None => databases.get(databaseName).map(_.getTable(tableName)) match {
        case Some(table) => tables.putIfAbsent(tableId, table).getOrElse(table)
        case None =>
          val newDB: Database = new Database(databaseName)
          val newTB: BTable = newTable
          val finalTB = databases.putIfAbsent(databaseName, newDB)
            .getOrElse(newDB)
            .setTable(tableName, newTB)
            .getOrElse(newTB)
          if (finalTB == newTB && tableId > 0) {
            tables.put(tableId, finalTB)
          }
          unknownTables.remove(tableId)
          finalTB
      }
    }
  }

  override def toString: String = {
    databases.values.mkString("Registry(", ", ", ")")
  }
}

class Database(name: String) {
  private val tables: TrieMap[String, BTable] = TrieMap.empty

  def setTable(name: String, table: BTable): Option[BTable] = tables.putIfAbsent(name, table)

  def getTable(name: String): BTable = tables(name)

  def getName: String = name

  override def toString: String = {
    tables.values.mkString(s"Database{$name: (", ", ", ")}")
  }
}


