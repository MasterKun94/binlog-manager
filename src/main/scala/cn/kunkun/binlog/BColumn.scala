package cn.kunkun.binlog

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType

case class BColumn(name: String, columnType: ColumnType)
