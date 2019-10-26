package cn.kunkun.binlog

import cn.kunkun.binlog.Events._
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

import scala.collection.JavaConversions.collectionAsScalaIterable

class EventHandler(registry: Registry) {
  private val eventRegistry = registry
  private var expandable = true

  def handleDefault(header: EventHeader, data: EventData): Traversable[BEvent] = Option(DefaultEvent(header.getTimestamp, header.getEventType, data))

  def handleTableMap(header: EventHeader, data: TableMapEventData): Traversable[BEvent] = {
    val table = eventRegistry.getOrRegistry(data.getDatabase, data.getTable, data.getTableId) {
      val seqBuilder = Array.newBuilder[BColumn]
      val types = data.getColumnTypes
      for (elem <- types.indices) {
        seqBuilder += BColumn(s"col_$elem", ColumnType.byCode(types(elem)))
      }
      BTable.of(data.getDatabase, data.getTable, seqBuilder.result())
    }
    Option(TableMapEvent(table, header.getTimestamp))
  }

  def handleWrite(header: EventHeader, data: WriteRowsEventData): Traversable[BEvent] = {
    val table = eventRegistry.getTable(data.getTableId) match {
      case Some(table0) => table0

      case None => eventRegistry.registryUnknownTable(data.getTableId, data.getRows.head)
    }
    data.getRows.map(elems => WriteEvent(table, header.getTimestamp, table.getRow(elems)))
  }

  def handleUpdate(header: EventHeader, data: UpdateRowsEventData): Traversable[BEvent] = {
    val table = eventRegistry.getTable(data.getTableId) match {
      case Some(table0) =>table0

      case None => eventRegistry.registryUnknownTable(data.getTableId, data.getRows.head.getKey)
    }
    data.getRows.map(elems => UpdateEvent(table, header.getTimestamp, table.getRow(elems.getKey), table.getRow(elems.getValue)))
  }

  def handleDelete(header: EventHeader, data: DeleteRowsEventData): Traversable[BEvent] = {
    val table = eventRegistry.getTable(data.getTableId) match {
      case Some(table0) =>table0

      case None => eventRegistry.registryUnknownTable(data.getTableId, data.getRows.head)
    }
    data.getRows.map(elems => DeleteEvent(table, header.getTimestamp, table.getRow(elems)))
  }

  def handleQuery(header: EventHeader, data: QueryEventData): Traversable[QueryEvent] = {
    Option(QueryEvent(data.getDatabase, header.getTimestamp, data.getSql))
  }

  def setExpandable(expandable: Boolean): Unit = {
    this.expandable = expandable
  }
}

object EventHandler {
  def apply(registry: Registry): EventHandler = new EventHandler(registry)
}
