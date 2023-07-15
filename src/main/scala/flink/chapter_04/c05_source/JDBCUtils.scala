package flink.chapter_04.c05_source


import java.sql.{Connection, DriverManager}

object JDBCUtils {

  private val driver = "com.mysql.jdbc.Driver"
  private val url = "jdbc:mysql://localhost:3306/student_db"
  private val username = "root"
  private val password = "123456"

  def getConnection(): Connection = {
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, password)
    connection
  }
}
