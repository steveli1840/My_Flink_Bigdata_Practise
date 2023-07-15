package flink.chapter_04.c05_source

import flink.chapter_04.c05_source.Domain.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.sql.{Connection, PreparedStatement}

class MySQLSource extends RichSourceFunction[Student] {
  var conn: Connection = _
  var ps: PreparedStatement = _
  var isRunning = true

  /**
   *
   * @param parameters key/value 轻量级配置对象
   */
  override def open(parameters: Configuration): Unit = {
    conn = JDBCUtils.getConnection()
    ps = conn.prepareStatement("select * from student")
  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    val rs = ps.executeQuery()
    while (isRunning && rs.next()) {
      val student = Student(rs.getInt("id"), rs.getString("name"), rs.getInt("age"))
      ctx.collect(student)
    }
  }

  override def cancel(): Unit = {
    this.isRunning = false
  }
}
