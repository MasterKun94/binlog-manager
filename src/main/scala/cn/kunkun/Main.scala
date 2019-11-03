package cn.kunkun

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import scala.io.StdIn


/**
  * Hello world!
  *
  */
object Main extends App {

//  val i = 1 -> 2 -> 3 -> 4 -> 5
//  val d = 1 :: 2 :: 3 :: 4 :: 5 :: Nil
//  println(i)
//  println(d)
//
//  val queue: BlockingQueue[Int] = new LinkedBlockingQueue[Int]()
//  val s = Iterator.iterate(0) {
//    i =>
//      Thread.sleep(1000)
//      i + 1
//  }
//
//  val t = {
//    Iterator continually queue.take
//  }
//    .map(_ * 2)
//
//  def print(): Unit = {
//    println(t.next())
//    print()
//  }
//  def add(): Unit = {
//    queue.put(s.next())
//    add()
//  }
//
//  ExecutionContext.global.execute(
//    new Runnable {
//      override def run(): Unit = add()
//    })
//
//  print()

  def schedule1(period: Long, accumulator: Long => Long = identity): Iterator[Long] =
    Iterator.iterate(System.currentTimeMillis()) {t =>
      accumulator(t) + period
    }

  def schedule2(format: String, accumulator: Long => Long = identity): Iterator[Long] = {
    val dayFormat = new SimpleDateFormat("yyyy-MM-dd")
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val _1day = 24 * 60 * 60 * 1000
    Iterator.iterate(System.currentTimeMillis()) {t =>
      val newFormat = dayFormat.format(new Date(accumulator(t))) + " " + format
      val value = timeFormat.parse(newFormat).getTime
      if (value <= t) value + _1day
      else value
    }
  }
  var isRunning = true

  def doPrintLoop(stream: Iterator[Long], isRunning: => Boolean = isRunning): Unit = {
    if (isRunning) {
      Thread.sleep(1000)
      println(new Date(stream.next()))
      doPrintLoop(stream)
    }
  }
  val c = Calendar.getInstance()
  c.setTime(new Date())
  println(c.get(Calendar.YEAR))
  println(c.get(Calendar.MONTH))
  println(c.get(Calendar.DATE))
  println(c.getTime)
  println(c.get(Calendar.DAY_OF_YEAR))
  println(c)

//  new Thread(new Runnable {
//    override def run(): Unit = doPrintLoop(schedule2("03:20:11"))
//  }).start()
  val iterator = schedule2("03:20:11")
//val iterator = schedule1(24 * 60 * 60 * 1000)
  doScan()
  def doScan(): Unit = {
    val value = StdIn.readLine()
    if (!value.equals("stop")) {
      println(new Date(iterator.next()))
      doScan()
    }
  }


}
