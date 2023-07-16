package flink.chapter_04.c09_partition

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestBroadcast {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds = env.fromElements(1, 2, 3, 4, 5, 6)
    val ds1 = ds.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask)
        value
      }
    })
    val ds2 = ds1.broadcast
    val ds3 = ds2.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("----元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask)
        value
      }
    })
    env.execute("ShuffleExample")
  }
}
