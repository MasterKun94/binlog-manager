package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.Event
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

object ImplicitUtil {

  implicit class ScalaBinaryLogClient(client: BinaryLogClient) {
    def registerListener(listener: Event => Unit): Unit = {
      client.registerEventListener(new BinaryLogClient.EventListener {
        override def onEvent(event: Event): Unit = listener(event)
      })
    }
  }

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
