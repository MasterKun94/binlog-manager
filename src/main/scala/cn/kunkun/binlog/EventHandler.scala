package cn.kunkun.binlog

import cn.kunkun.binlog.Events._
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

import scala.collection.JavaConversions.collectionAsScalaIterable

class EventHandler(registry: Registry) {
  private val eventRegistry = registry

  def handleDefault(header: EventHeader, data: EventData): Option[BEvent] = Option(DefaultEvent(header.getTimestamp, header.getEventType, data))

  def handleTableMap(header: EventHeader, data: TableMapEventData): Option[BEvent] = {
    val table = eventRegistry.getOrRegistry(data.getDatabase, data.getTable, data.getTableId) {
      val seqBuilder = Array.newBuilder[Column]
      val types = data.getColumnTypes
      for (elem <- types.indices) {
        seqBuilder += Column(s"col_$elem", ColumnType.byCode(types(elem)))
      }
      BTable(data.getTable, data.getDatabase, seqBuilder.result())
    }
    Option(TableMapEvent(table, header.getTimestamp))
  }

  def handleWrite(header: EventHeader, data: WriteRowsEventData): Iterable[BEvent] = {
    println(eventRegistry.toString)
    val table = eventRegistry.getTable(data.getTableId) match {
      case Some(table0) =>table0

      case None => eventRegistry.registryUnknownTable(data.getTableId, data.getRows.head)
    }
    data.getRows.map(elems => WriteEvent(table, header.getTimestamp, table.newRow(elems)))
  }

  def handleUpdate(header: EventHeader, data: UpdateRowsEventData): Iterable[BEvent] = {
    val table = eventRegistry.getTable(data.getTableId) match {
      case Some(table0) =>table0

      case None => eventRegistry.registryUnknownTable(data.getTableId, data.getRows.head.getKey)
    }
    data.getRows.map(elems => UpdateEvent(table, header.getTimestamp, table.newRow(elems.getKey), table.newRow(elems.getValue)))
  }

  def handleDelete(header: EventHeader, data: DeleteRowsEventData): Iterable[BEvent] = {
    val table = eventRegistry.getTable(data.getTableId) match {
      case Some(table0) =>table0

      case None => eventRegistry.registryUnknownTable(data.getTableId, data.getRows.head)
    }
    data.getRows.map(elems => DeleteEvent(table, header.getTimestamp, table.newRow(elems)))
  }
}

object EventHandler {
  def apply(registry: Registry): EventHandler = new EventHandler(registry)
}
