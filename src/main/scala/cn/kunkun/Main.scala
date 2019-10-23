package cn.kunkun

import cn.kunkun.binlog.{BConfig, BinlogClient, EventVisitor, Events}

/**
 * Hello world!
 *
 */
object Main extends App {
  BinlogClient(
    BConfig(_
      .setHostname("myubuntu")
      .setPassword("123456")
      .setUsername("kunkun")
      .setPort(3306)
    )
  ).addVisitor(
    new EventVisitor {
      override def visitDefault(event: Events.BEvent): Unit = println(event)

      override def visit(event: Events.TableMapEvent): Unit = println(event)

      override def visit(event: Events.WriteEvent): Unit = println(event)

      override def visit(event: Events.UpdateEvent): Unit = println(event)

      override def visit(event: Events.DeleteEvent): Unit = println(event)

      override def visit(event: Events.QueryEvent): Unit = println(event)
    }
  ).connect()
}
