package flink.chapter_04.c10_window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 每隔5秒计算每个用户的最小成绩
 */
object TestProcessWindowReduce {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.socketTextStream("localhost", 9999)
    //用户ID,成绩
    //1001,99
    //1002,87
    //1001,76
    //1001,88
    //1002,69
    ds.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toInt)
    })
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .reduce(new MyReducefunction2, new MyProcessWindowsFunction2)
      .print()
    env.execute("TestProcessWindowReduce")
  }

  class MyReducefunction2 extends ReduceFunction[(String, Int)] {
    override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
      if (value1._2 > value2._2) {
        (value1._1, value2._2)
      } else {
        (value1._1, value1._2)
      }
    }
  }

  class MyProcessWindowsFunction2 extends ProcessWindowFunction[(String, Int), (String, String), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, String)]): Unit = {
      val min = elements.iterator.next().toString()
      out.collect(context.window.getStart.toString, min)
    }
  }
}
