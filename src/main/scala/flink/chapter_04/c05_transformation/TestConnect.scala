package flink.chapter_04.c05_transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestConnect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(1, 2, 3, 4)
    val ds2 = env.fromElements("a", "b", "c", "d")
    val connectedDS = ds1.connect(ds2)
    val resultDs = connectedDS.map(new CoMapFunction[Int, String, String] {
      override def map1(value: Int): String = value.toString

      override def map2(value: String): String = value
    })
    resultDs.print()
    env.execute("Test connect")
  }
}
