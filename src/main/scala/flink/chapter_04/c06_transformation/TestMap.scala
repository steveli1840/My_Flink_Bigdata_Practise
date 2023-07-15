package flink.chapter_04.c06_transformation

import org.apache.flink.streaming.api.scala._

object TestMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(List(1, 2, 3, 4, 5))
    val ds2 = ds1.map(x => x + 1)
    val ds3 = ds1.map(_ + 1)
    ds3.print()
    env.execute("TestMap")
  }
}
