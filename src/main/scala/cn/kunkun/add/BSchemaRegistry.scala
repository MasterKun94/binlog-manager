package cn.kunkun.add

import scala.collection.concurrent.TrieMap

class BSchemaRegistry(name: String) {

  private val schemaMap: TrieMap[Symbol, BSchema] = TrieMap.empty

  def getSchema(schemaName: Symbol): Option[BSchema] = schemaMap.get(schemaName)

  def setSchemaIfAbsent(schema: BSchema): Option[BSchema] = {
    schemaMap.putIfAbsent(schema.schemaName, schema.registerFrom(this))
  }

  def setSchemaIfAbsentAndGet(schema: BSchema): BSchema = {
    setSchemaIfAbsent(schema) match {
      case Some(s) => s
      case None => getSchema(schema.schemaName).get
    }
  }

  def getTable(schemaName: Symbol, tableName: Symbol): Option[BTable] = {
    getSchema(schemaName) match {
      case Some(schema) => schema.getTable(tableName)

      case None => throw new IllegalArgumentException(s"schema $schemaName not exist")
    }
  }

  def setTableIfAbsent(table: BTable): Option[BTable] = {
    getSchema(table.schemaName) match {
      case Some(schema) => schema.setTableIfAbsent(table)

      case None => throw new IllegalArgumentException(s"schema ${table.getSchema.schemaName} not exist")
    }
  }

  def setTableIfAbsentAndGet(table: BTable): BTable = {
    getSchema(table.schemaName) match {
      case Some(schema) =>schema.setTableIfAbsentAndGet(table)

      case None => throw new IllegalArgumentException(s"schema ${table.schemaName} not exist")
    }
  }

  override def toString: String = {
    schemaMap.values.mkString(s"Registry($name)[", ", ", "]")
  }
}
