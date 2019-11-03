package cn.kunkun.add

import BColumn.Type.ColumnType

object ImplicitUtil {

  implicit def str2Symbol(str: String): Symbol = Symbol(str)

  implicit class ColumnConverter(columnName: String) {
    def -> (columnType: ColumnType): BColumn = {
      BColumn(Symbol(columnName), columnType)
    }

    def -> (columnType: String): BColumn = -> (BColumn.Type.withName(columnType.toUpperCase))
  }

  implicit class SymbolConverter(columnName: Symbol) {

    def -> (columnType: Symbol): BColumn = BColumn(columnName, BColumn.Type.withName(columnType.name.toUpperCase))
  }
}
