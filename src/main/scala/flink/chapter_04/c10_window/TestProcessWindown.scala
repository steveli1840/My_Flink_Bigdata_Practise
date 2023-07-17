package flink.chapter_04.c10_window

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 每隔5分钟统计每个用户产生的日志数量（全量聚合）
 */
object TestProcessWindown {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9999)
    //用户ID,日志内容
    //1001,login
    //1002,add
    //1001,update
    //1001,delete
    stream.map(line => {
      println(line)
      val arr = line.split(",")
      (arr(0), arr(1))
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.minutes(1))).process(new MyProcessWindowsFunc).print()
    env.execute("Process Window Function Example")
  }

  class MyProcessWindowsFunc extends ProcessWindowFunction[(String, String), (String, Int), String, TimeWindow] {


    override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[(String, Int)]): Unit = {
      out.collect(key, elements.size)
    }
  }
}

