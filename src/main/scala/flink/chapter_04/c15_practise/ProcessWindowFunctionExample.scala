package flink.chapter_04.c15_practise

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 案例分析：计算5秒内输入的单词数量
 */
object ProcessWindowFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9999)

    stream.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1))
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.minutes(2))).process(new MyProcessWindowFunc).print()
    env.execute("ProcessWindowFunctionExample")
  }
}

class MyProcessWindowFunc extends ProcessWindowFunction[(String, String), (String, Int), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[(String, Int)]): Unit = out.collect(key, elements.size)
}
