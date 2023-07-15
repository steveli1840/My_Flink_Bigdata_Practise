package flink.chapter_04.c06_transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestKeyBy {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(List(("zhangsan", 98), ("lisi", 88), ("wangwu", 78), ("zhangsan", 68), ("lisi", 58), ("wangwu", 48)))
    val keyedStream = ds1.keyBy(_._1)
    keyedStream.print()
    env.execute("TestKeyBy")
  }
}
