package cn.kunkun.calcite

import cn.kunkun.add.{BSchema, BSchemaRegistry, BTable}
import org.apache.calcite.schema.SchemaPlus

object ImplicitUtil {
  implicit def asCalciteSchema(database: BSchema): MySchema = new MySchema(database)

  implicit def asCalciteTable(table: BTable): MyTable = new MyTable(table)

  implicit class MySchemaPlus(schemaPlus: SchemaPlus) {
    def registryFrom(registry: BSchemaRegistry): SchemaPlus = {
      for (schema <- registry.schemas) {
        schemaPlus.add(schema.schemaName.name, schema)
      }
      schemaPlus
    }
  }

}
