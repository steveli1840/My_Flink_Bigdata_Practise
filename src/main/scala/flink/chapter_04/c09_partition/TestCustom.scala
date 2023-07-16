package flink.chapter_04.c09_partition

import org.apache.flink.api.common.functions.{Partitioner, RichMapFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestCustom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val arr = Array(
      "chinese,98",
      "math,88",
      "english,96"
    )
    val ds = env.fromCollection(arr)
    val ds1 = ds.map(new RichMapFunction[String, (String, Int)] {
      override def map(value: String): (String, Int) = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask)
        (value.split(",")(0), value.split(",")(1).toInt)
      }
    }).setParallelism(2)

    val ds2 = ds1.partitionCustom(new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        if (key.equals("chinese")) {
          0
        } else if (key.equals("math")) {
          1
        } else {
          2
        }
      }
    }, v => v._1)

    ds2.map(new RichMapFunction[(String, Int), (String, Int)] {
      override def map(value: (String, Int)): (String, Int) = {
        println("---元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask)
        value
      }
    }).setParallelism(3).print()
    env.execute("ShuffleExample")
  }

}
