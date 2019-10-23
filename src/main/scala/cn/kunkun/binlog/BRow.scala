package cn.kunkun.binlog

import java.io.{Serializable => JSerializable}

class BRow(table: BTable) {
  val columns: Seq[Column] = table.getColumns
  val elems: Array[java.io.Serializable] = new Array(columns.length)
  val map: Map[String, Int] = {
    val builder = Map.newBuilder[String, Int]
    for (i <- columns.indices) {
      builder += ((columns(i).name, i))
    }
    builder.result()
  }

  def getSchema: BTable = table

  def set(values: Array[JSerializable]): Unit = Array.copy(values, 0, elems, 0, elems.length)

  def get[T](index: Int): T = getElem(index).asInstanceOf[T]

  def getString(index: Int): String = getElem(index) match {
    case array: Array[Byte] => new String(array)
    case elem => elem.toString
  }

  def toBytes(index: Int): Array[Byte] = get[Array[Byte]](index)

  def toInt(index: Int): Int = get[Int](index)

  def toLong(index: Int): Long = get[Long](index)

  def toBoolean(index: Int): Boolean = get[Boolean](index)

  def toChar(index: Int): Char = get[Char](index)

  def toShort(index: Int): Short = get[Short](index)

  def toDouble(index: Int): Double = get[Double](index)

  def toFloat(index: Int): Float = get[Float](index)

  def get[T](column: String): T = getElem(column).asInstanceOf[T]

  def getString(column: String): String = get(map(column))

  def toBytes(column: String): Array[Byte] = get[Array[Byte]](column)

  def toInt(column: String): Int = get[Int](column)

  def toLong(column: String): Long = get[Int](column)

  def toBoolean(column: String): Boolean = get[Boolean](column)

  def toChar(column: String): Char = get[Char](column)

  def toShort(column: String): Short = get[Short](column)

  def toDouble(column: String): Double = get[Double](column)

  def toFloat(column: String): Float = get[Float](column)

  private def getElem(index: Int): JSerializable = elems(index)

  private def getElem(column: String): JSerializable = getElem(map(column))

  override def toString: String = {
    val stringBuilder = new StringBuilder
    var isFirst = true
    for (i <- columns.indices) {
      if (isFirst) {
        stringBuilder.append("Row(")
        isFirst = false
      } else {
        stringBuilder.append(", ")
      }
      stringBuilder
        .append(columns(i).name)
        .append(": ")
        .append(getString(i))
    }
    stringBuilder.toString()
  }
}

object BRow {
  def apply(table: BTable): BRow = new BRow(table)

}
