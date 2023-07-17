package flink.chapter_04.c10_window

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }.map {
      (_, 1)
    }
      .keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1).print()
    env.execute("Window Stream word count")
  }
}
