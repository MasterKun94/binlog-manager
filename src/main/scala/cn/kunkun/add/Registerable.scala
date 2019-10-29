package cn.kunkun.add

trait Registerable[T, OUT] {
  def registerFrom(t: T): OUT
}
