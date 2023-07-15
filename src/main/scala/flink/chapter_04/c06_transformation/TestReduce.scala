package flink.chapter_04.c06_transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestReduce {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(List(("zhangsan", 98), ("lisi", 88), ("wangwu", 78), ("zhangsan", 68), ("lisi", 58), ("wangwu", 48)))
    val kds = ds1.keyBy(_._1)
    val reducedDs = kds.reduce((t1, t2) => {
      (t1._1, t1._2 + t2._2)
    })
    reducedDs.print()
    env.execute("Test Reduce")
  }

}
