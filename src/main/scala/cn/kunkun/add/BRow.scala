package cn.kunkun.add

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

  def get[T](column: Symbol): T = getElem(column).asInstanceOf[T]

  def getString(column: Symbol): String = get[String](column)

  def getBytes(column: Symbol): Array[Byte] = get[Array[Byte]](column)

  def getInt(column: Symbol): Int = get[Int](column)

  def getLong(column: Symbol): Long = get[Int](column)

  def getBoolean(column: Symbol): Boolean = get[Boolean](column)

  def getChar(column: Symbol): Char = get[Char](column)

  def getShort(column: Symbol): Short = get[Short](column)

  def getDouble(column: Symbol): Double = get[Double](column)

  def getFloat(column: Symbol): Float = get[Float](column)

  private def getElem(index: Int): JSerializable = elems(index)

  private def getElem(column: Symbol): JSerializable = getElem(table.getIndexByColumn(column))

  override def toString: String = {
    table.getColumns
      .map(col => s"${col.getName} ${col.getType}")
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
