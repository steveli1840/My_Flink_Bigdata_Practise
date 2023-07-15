package flink.chapter_04.c06_transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(List("hadoop hello scala", "flink hello"))
    val ds2 = ds1.flatMap(_.split(" "))
    ds2.print()
    env.execute("TestFlatMap")
  }
}
