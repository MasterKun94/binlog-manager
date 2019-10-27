package cn.kunkun.binlog

trait Registerable[T, OUT] {
  def registerFrom(t: T): OUT
}
