package cn.kunkun.add

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType


object ImplicitUtil {

  implicit def str2Symbol(str: String): Symbol = Symbol(str)

  implicit class ColumnConverter(columnName: String) {
    def -> (columnType: ColumnType): BColumn = {
      BColumn(Symbol(columnName), columnType)
    }

    def -> (columnType: String): BColumn = -> (ColumnType.valueOf(columnType.toUpperCase))
  }

  implicit class SymbolConverter(columnName: Symbol) {

    def -> (columnType: Symbol): BColumn = BColumn(columnName, ColumnType.valueOf(columnType.name.toUpperCase))
  }
}
