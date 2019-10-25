package cn.kunkun.calcite

import java.util

import com.google.common.collect.Multimap
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.rel.`type`.RelProtoDataType
import org.apache.calcite.schema
import org.apache.calcite.schema.{Schema, SchemaPlus, SchemaVersion, Table}
import org.apache.calcite.schema.impl.AbstractSchema

class MySchema extends AbstractSchema {
  override def getTableMap: util.Map[String, Table] = super.getTableMap

  override def getTypeMap: util.Map[String, RelProtoDataType] = super.getTypeMap

  override def getTypeNames: util.Set[String] = super.getTypeNames

  override def getType(name: String): RelProtoDataType = super.getType(name)

  override def getExpression(parentSchema: SchemaPlus, name: String): Expression = super.getExpression(parentSchema, name)

  override def getSubSchemaMap: util.Map[String, Schema] = super.getSubSchemaMap

  override def isMutable: Boolean = super.isMutable

  override def getFunctionMultimap: Multimap[String, schema.Function] = super.getFunctionMultimap

  override def snapshot(version: SchemaVersion): Schema = super.snapshot(version)
}
