package cn.kunkun.binlog

import scala.collection.mutable

class BConfig {

  private val map: mutable.Map[String, Any] = mutable.Map.empty

  def getHostname: String = get("hostname")

  def getPort: Int = get("port")

  def getSchema: String = get("schema")

  def getPassword: String = get("password")

  def getUsername: String = get("username")

  def setHostname(hostname: String): BConfig = set("hostname", hostname)

  def setPort(port: Int): BConfig = set("port", port)

  def setSchema(schema: String): BConfig = set("schema", schema)

  def setPassword(password: String): BConfig = set("password", password)

  def setUsername(url: String): BConfig = set("username", url)

  def get[T](key: String): T = map.get(key).map(_.asInstanceOf[T]).getOrElse(null.asInstanceOf[T])

  def set(key: String, value: Any): BConfig = {
    map.put(key, value)
    this
  }
}

object BConfig {
  def apply(): BConfig = new BConfig()

  def apply(map: BConfig => BConfig): BConfig = map(new BConfig())
}
