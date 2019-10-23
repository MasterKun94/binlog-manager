package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.Event

object ImplicitUtil {

  implicit class ScalaBinaryLogClient(client: BinaryLogClient) {
    def registerListener(listener: Event => Unit): Unit = {
      client.registerEventListener(new BinaryLogClient.EventListener {
        override def onEvent(event: Event): Unit = listener(event)
      })
    }
  }
}
