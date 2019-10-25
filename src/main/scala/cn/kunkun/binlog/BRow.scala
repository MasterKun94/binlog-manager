package cn.kunkun.binlog

import java.io.{Serializable => JSerializable}

class BRow(table: BTable, elems: Array[JSerializable]) {

  def getTable: BTable = table

  def set(values: Array[JSerializable]): Unit = Array.copy(values, 0, elems, 0, elems.length)

  def get[T](index: Int): T = getElem(index).asInstanceOf[T]

  def getString(index: Int): String = getElem(index) match {
    case array: Array[Byte] => new String(array)
    case elem => elem.toString
  }

  def getBytes(index: Int): Array[Byte] = get[Array[Byte]](index)

  def getInt(index: Int): Int = get[Int](index)

  def getLong(index: Int): Long = get[Long](index)

  def getBoolean(index: Int): Boolean = get[Boolean](index)

  def getChar(index: Int): Char = get[Char](index)

  def getShort(index: Int): Short = get[Short](index)

  def getDouble(index: Int): Double = get[Double](index)

  def getFloat(index: Int): Float = get[Float](index)

  def get[T](column: String): T = getElem(column).asInstanceOf[T]

  def getString(column: String): String = get[String](column)

  def getBytes(column: String): Array[Byte] = get[Array[Byte]](column)

  def getInt(column: String): Int = get[Int](column)

  def getLong(column: String): Long = get[Int](column)

  def getBoolean(column: String): Boolean = get[Boolean](column)

  def getChar(column: String): Char = get[Char](column)

  def getShort(column: String): Short = get[Short](column)

  def getDouble(column: String): Double = get[Double](column)

  def getFloat(column: String): Float = get[Float](column)

  private def getElem(index: Int): JSerializable = elems(index)

  private def getElem(column: String): JSerializable = getElem(table.getIndexByColumn(column))

  override def toString: String = {
    table.getColumns
      .map(col => s"${col.name} ${col.columnType}")
      .mkString("Row(", ", ", ")")
  }

  def copy(): BRow = {
    val array = new Array[JSerializable](elems.length)
    Array.copy(elems, 0, array, 0, elems.length)
    new BRow(table, array)
  }
}

object BRow {

  def apply(table: BTable)(elements: Array[JSerializable]): BRow = new BRow(table, elements)

}
