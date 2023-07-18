package flink.chapter_04.c14_practise

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 案例分析：计算5秒内输入的单词数量
 */
object WindowExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }.map {
      (_, 1)
    }.keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1)

    counts.print()
    env.execute("Windows Stream WordCount")
  }

}
