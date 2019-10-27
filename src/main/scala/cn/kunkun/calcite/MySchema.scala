package cn.kunkun.calcite

import java.util

import cn.kunkun.binlog.Database
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelProtoDataType}
import org.apache.calcite.schema
import org.apache.calcite.schema._
import ImplicitUtil.asCalciteTable
import cn.kunkun.binlog.ImplicitUtil.str2Symbol

import scala.collection.JavaConversions._

class MySchema(database: Database) extends Schema {
  val functions: Map[String, schema.Function] = Map.empty.withDefaultValue(null)
  val subSchema: Map[String, Schema] = Map.empty.withDefaultValue(null)

  override def getTable(name: String): Table = database.getTable(name).get

  override def getTableNames: util.Set[String] = database.getTableNames.map(_.name)

  override def getType(name: String): RelProtoDataType = new RelProtoDataType {
    override def apply(factory: RelDataTypeFactory): RelDataType = getTable(name).getRowType(factory)
  }

  override def getTypeNames: util.Set[String] = getTableNames.map(name => getTable(name).getJdbcTableType.jdbcName)

  override def getFunctions(name: String): util.Collection[schema.Function] = functions.values //TODO

  override def getFunctionNames: util.Set[String] = functions.keySet //TODO

  override def getSubSchema(name: String): Schema = subSchema(name)

  override def getSubSchemaNames: util.Set[String] = subSchema.keySet

  override def getExpression(parentSchema: SchemaPlus, name: String): Expression = {
    Schemas.subSchemaExpression(parentSchema, name, classOf[MySchema])
  }

  override def isMutable: Boolean = true

  override def snapshot(version: SchemaVersion): Schema = this //TODO
}
