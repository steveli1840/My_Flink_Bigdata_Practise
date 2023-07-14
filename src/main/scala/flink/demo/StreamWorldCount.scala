package flink.demo

import org.apache.flink.streaming.api.scala._

object StreamWorldCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[String] = env.socketTextStream("localhost", 9999)
    val result = data.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(_._1).sum(1)
    result.print()
    env.execute("StreamWordCount")
  }
}
