package cn.kunkun.add

trait Registerable[T, OUT] {
  protected def unSupport: Nothing = throw new UnsupportedOperationException

  def registerFrom(t: T): OUT
}
