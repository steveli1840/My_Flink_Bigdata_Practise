package flink.chapter_04.c06_transformation

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestReduce1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(List(("zhangsan", 98), ("lisi", 88), ("wangwu", 78), ("zhangsan", 68), ("lisi", 58), ("wangwu", 48)))
    val kds = ds1.keyBy(_._1)
    val reducedDS = kds.reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    })
    reducedDS.print()
    env.execute("TestReduce1")
  }

}
