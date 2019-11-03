package cn.kunkun.add

import scala.collection.concurrent.TrieMap

class BSchema(name: Symbol) extends Registerable[BSchemaRegistry, BSchema] {
  private val tableMap: TrieMap[Symbol, BTable] = TrieMap.empty
  protected def getTableMap: TrieMap[Symbol, BTable] = tableMap

  def schemaName: Symbol = name

  def getTable(tableName: Symbol): Option[BTable] = getTableMap.get(tableName)

  def setTableIfAbsent(table: BTable): Option[BTable] = {
    getTableMap.putIfAbsent(table.tableName, table.registerFrom(this))
  }

  def setTableIfAbsentAndGet(table: BTable): BTable = {
    setTableIfAbsent(table) match {
      case Some(tb) => tb

      case None => getTable(table.tableName).get

    }
  }

  def tableNames: Set[Symbol] = getTableMap.keySet.toSet

  def getRegistry: BSchemaRegistry = throw new UnsupportedOperationException

  override def toString: String = {
    getTableMap.values.mkString(s"Schema[$name: (", ", ", ")]")
  }


  override def registerFrom(t: BSchemaRegistry): BSchema = new BSchema(name) {

    override def getTableMap: TrieMap[Symbol, BTable] = tableMap

    override def getRegistry: BSchemaRegistry = t
  }
}

object BSchema {
  def of(name: String)(implicit registry: BSchemaRegistry): BSchema = {
    registry.setSchemaIfAbsentAndGet(new BSchema(Symbol(name)))
  }
}
