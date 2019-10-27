package cn.kunkun

import cn.kunkun.binlog.Statistics.PrimaryKey
import cn.kunkun.binlog._
import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

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
    ))
    .register(
      BTable('test, 'user)(
        BColumn('id, ColumnType.LONG),
        BColumn('name, ColumnType.VARCHAR),
        BColumn('age, ColumnType.LONG),
        BColumn('hobby, ColumnType.VARCHAR)
      )(
        PrimaryKey("id")
    ))
    .addVisitor(
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
