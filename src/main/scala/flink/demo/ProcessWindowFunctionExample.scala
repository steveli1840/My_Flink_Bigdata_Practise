package flink.demo

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object ProcessWindowFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var stream = env.socketTextStream("localhost", 9999)
    stream.map(line=>{
      var arr = line.split(",")
      (arr(0),arr(1))
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.minutes(5))).process(new MyProcessWindowFunction).print()
    env.execute("ProcessWindowFunctionExample")
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(String,String),(String,Int),String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[(String, Int)]): Unit = {
      out.collect(key,elements.size)
    }
  }
}
