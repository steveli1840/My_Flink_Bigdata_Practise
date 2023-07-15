package flink.chapter_01

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)
    data.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(_._1).sum(1).print()
    env.execute("StreamWordCount")
  }
}
