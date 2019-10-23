package cn.kunkun.binlog

import cn.kunkun.binlog.Events.BEvent
import cn.kunkun.binlog.ImplicitUtil.ScalaBinaryLogClient
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer

import scala.collection.mutable

class BinlogClient(bConfig: BConfig) {

  private val handler = EventHandler(new Registry)
  private val client = new BinaryLogClient(bConfig.getHostname, bConfig.getPort, bConfig.getSchema, bConfig.getUsername, bConfig.getPassword)
  private val visitors: mutable.Queue[EventVisitor] = mutable.Queue.empty

  val eventDeserializer = new EventDeserializer
  eventDeserializer.setCompatibilityMode(
    EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
    EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY)
  client.setEventDeserializer(eventDeserializer)

  client.registerListener(event => {
    val header = event.getHeader[EventHeader]
//    println(event)
    val eventTraversable: Traversable[BEvent] = event.getData[EventData] match {
      case tableMap: TableMapEventData =>
        handler.handleTableMap(header, tableMap)

      case delete: DeleteRowsEventData =>
        handler.handleDelete(header, delete)

      case update: UpdateRowsEventData =>
        handler.handleUpdate(header, update)

      case insert: WriteRowsEventData =>
        handler.handleWrite(header, insert)

      case default =>
        handler.handleDefault(header, default)
    }
    eventTraversable.foreach(event => visitors.foreach(visitor => event.accept(visitor)))
  })

  def connect(): Unit = {
    client.connect()
  }

  def addVisitor(visitor: EventVisitor): BinlogClient = {
    visitors += visitor
    this
  }
}

object BinlogClient {
  def apply(bConfig: BConfig): BinlogClient = new BinlogClient(bConfig)
}


