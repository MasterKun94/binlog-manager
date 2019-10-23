package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.event.{EventData, EventType}

object Events {
  trait BEvent {
    def accept(visitor: EventVisitor)
  }

  case class DefaultEvent(timestamp: Long, eventType: EventType, data: EventData) extends BEvent {
    override def accept(visitor: EventVisitor): Unit = visitor.visitDefault(this)
  }

  case class WriteEvent(table: BTable, timestamp: Long, row: BRow) extends BEvent {
    override def accept(visitor: EventVisitor): Unit = visitor.visit(this)
  }

  case class DeleteEvent(table: BTable, timestamp: Long, row: BRow) extends BEvent {
    override def accept(visitor: EventVisitor): Unit = visitor.visit(this)
  }

  case class UpdateEvent(table: BTable, timestamp: Long, oldRow: BRow, newRow: BRow) extends BEvent {
    override def accept(visitor: EventVisitor): Unit = visitor.visit(this)
  }

  case class QueryEvent(table: BTable, timestamp: Long, sql: String) extends BEvent {
    override def accept(visitor: EventVisitor): Unit = visitor.visit(this)
  }

  case class TableMapEvent(table: BTable, timestamp: Long) extends BEvent {
    override def accept(visitor: EventVisitor): Unit = visitor.visit(this)
  }
}