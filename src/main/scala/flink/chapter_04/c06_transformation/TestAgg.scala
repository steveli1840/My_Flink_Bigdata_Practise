package flink.chapter_04.c06_transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestAgg {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      (0, 0, 2),
      (0, 1, 1),
      (0, 3, 0),
      (1, 0, 8),
      (1, 1, 5),
      (1, 3, 6)
    )
    ds.keyBy(_._1)
      //      .sum(1)
      //      .min(1)
      //      .max(1)
      //      .minBy(1)
      /**
       * maxBy()与max()算子的区别在于，maxBy()返回的是指定字段中最大值所在的整个元素，即maxBy()可以得到数据流中最大的元素。例如以下代码，按第一个字段分组，对第二个字段求最大值：
       */
      .maxBy(1)
      .print()
    env.execute("TestAgg")
  }

}
