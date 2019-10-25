package cn.kunkun.binlog

import cn.kunkun.binlog.Events._

trait EventVisitor {
  def visitDefault(event: BEvent) {}

  def visit(event: TableMapEvent) {}

  def visit(event: WriteEvent) {}

  def visit(event: UpdateEvent) {}

  def visit(event: DeleteEvent) {}

  def visit(event: QueryEvent) {}
}
