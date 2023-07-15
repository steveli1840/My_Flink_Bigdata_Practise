package flink.chapter_04.c06_transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(List(1, 2, 3, 4, 5, 6))
    val ds2 = ds1.filter(_ > 3)
    ds2.print()
    env.execute("TestFilter")
  }

}
