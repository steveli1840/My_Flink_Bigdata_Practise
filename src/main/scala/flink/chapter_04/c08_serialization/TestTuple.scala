package flink.chapter_04.c08_serialization

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestTuple {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(
      ("hello", 1),
      ("word", 2)
    )
    val ds2 = ds1.keyBy(1)
    ds2.print()
    env.execute("Test Tuple")
  }
}
