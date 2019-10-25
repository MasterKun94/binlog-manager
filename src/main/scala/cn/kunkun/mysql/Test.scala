package cn.kunkun.mysql

object Test extends App {

  import java.sql.Connection
  import java.sql.DriverManager
  import java.sql.ResultSet

  Class.forName("com.mysql.jdbc.Driver")

  // change user and password as you need it
  val con: Connection = DriverManager.getConnection("jdbc:mysql://192.168.10.138/backuptest", "chenmingkun", "123456")

  val rs: ResultSet = con.getMetaData.getTableTypes

  while (rs.next)

    println("TABLE_CAT = " + rs)
}
