package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

import scala.collection.concurrent.TrieMap

import java.io.{Serializable => JSerializable}

import ImplicitUtil.str2Symbol

class Registry(name: String) {
  private val databases: TrieMap[Symbol, Database] = TrieMap.empty
  private val tables: TrieMap[Long, BTable] = TrieMap()
  private val unknownTables: TrieMap[Long, BTable] = TrieMap()

  def getDatabase(name: Symbol): Option[Database] = databases.get(name)

  def getDatabaseMap: Map[Symbol, Database] = databases.toMap

  def getTable(databaseName: Symbol, tableName: Symbol): Option[BTable] = databases.get(databaseName).flatMap(_.getTable(tableName))

  def getTable(tableId: Long): Option[BTable] = {
    tables.get(tableId).orElse(unknownTables.get(tableId))
  }

  def registryUnknownTable(tableId: Long, array: Array[JSerializable]): BTable = {
    val columns = new Array[BColumn](array.length)
    columns.indices.foreach(i => columns(i) = BColumn(s"col_$i", ColumnType.NULL))
    val ukTable = BTable.of('unknown_db, s"unknown_tb_$tableId", columns)
    unknownTables.putIfAbsent(tableId, ukTable) match {
      case Some(table) => table
      case None => ukTable
    }
  }

  def getOrRegistry(databaseName: Symbol, tableName: Symbol, tableId: Long)(newTable: => BTable): BTable = {
    tables.get(tableId) match {
      case Some(table) => table
      case None => databases.get(databaseName).flatMap(_.getTable(tableName)) match {
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

  def registryTable(table: BTable): BTable = {
    getOrRegistry(table.databaseName, table.tableName, -1)(table)
  }

  override def toString: String = {
    databases.values.mkString(s"Registry($name)[", ", ", "]")
  }
}

object Registry {
  lazy val default: Registry = new Registry("default")

  def DEFAULT: Registry = default
}

class Database(name: Symbol) {
  private val tables: TrieMap[Symbol, BTable] = TrieMap.empty

  def setTable(name: Symbol, table: BTable): Option[BTable] = tables.putIfAbsent(name, table)

  def getTable(name: Symbol): Option[BTable] = tables.get(name)

  def getTableNames: collection.Set[Symbol] = tables.keySet

  def getName: Symbol = name

  override def toString: String = {
    tables.values.mkString(s"Database[$name: (", ", ", ")]")
  }
}


