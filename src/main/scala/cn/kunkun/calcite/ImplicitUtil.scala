package cn.kunkun.calcite

import cn.kunkun.binlog.{BTable, Database, Registry}
import org.apache.calcite.schema.SchemaPlus

object ImplicitUtil {
  implicit def asCalciteSchema(database: Database): MySchema = new MySchema(database)

  implicit def asCalciteTable(table: BTable): MyTable = new MyTable(table)

  implicit class MySchemaPlus(schemaPlus: SchemaPlus) {
    def registryFrom(registry: Registry): SchemaPlus = {
      for (tuple <- registry.getDatabaseMap) {
        schemaPlus.add(tuple._1.name, tuple._2)
      }
      schemaPlus
    }
  }

}
