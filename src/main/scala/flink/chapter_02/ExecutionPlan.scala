package flink.chapter_02

import org.apache.flink.streaming.api.scala._

object ExecutionPlan {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)
    data.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(_._1).sum(1).print().setParallelism(3)
    println(env.getExecutionPlan)
  }
  //将上述JSON字符串粘贴到可视化工具网址(https://wints.github.io/flink-web//visualizer/)提供的文本框中，可将JSON字符串解析为可视化图，该可视化图对应的是StreamGraph
}
