package flink.chapter_04.c05_source

import flink.chapter_04.c05_source.Domain.Student
import org.apache.flink.streaming.api.scala._

object StreamTest {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[Student] = senv.addSource(new MySQLSource)
    ds.print()
    senv.execute("StreamMySqlSource")
  }
}
