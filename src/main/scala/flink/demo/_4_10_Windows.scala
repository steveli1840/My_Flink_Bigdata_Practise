package flink.demo

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object _4_10_Windows {
  private def testBasicWindow(env: StreamExecutionEnvironment) = {
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

  def testProcessWindowFunc(env: StreamExecutionEnvironment): Unit = {
    val stream = env.socketTextStream("localhost", 9999)
    //用户ID,日志内容
    //1001,login
    //1002,add
    //1001,update
    //1001,delete
    stream.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1))
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.minutes(1))).process(new MyProcessWindowFunc).print()
    env.execute("testProcessWindowFunc")
  }

  def testProcessWindowReduceFunc(env: StreamExecutionEnvironment): Unit = {
    val stream = env.socketTextStream("localhost", 9999)
    stream.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toInt)
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).reduce(new MyReduceFunc2, new MyProcessWindowFunc2).print()
    env.execute("testProcessWindowReduceFunc")
  }

  def testAggFunc(env: StreamExecutionEnvironment): Unit = {
    val stream = env.socketTextStream("localhost", 9999)
    stream.map(line => {
      (line.split(",")(0), line.split(",")(1).toDouble)
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).aggregate(new MyAggFunc).print("output")
    env.execute("testAggFunc")
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    testBasicWindow(env)
    //    testProcessWindowFunc(env)
    //    testProcessWindowReduceFunc(env)
    testAggFunc(env)
  }

  class MyProcessWindowFunc extends ProcessWindowFunction[(String, String), (String, Int), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[(String, Int)]): Unit = {
      out.collect(key, elements.size)
    }
  }

  class MyReduceFunc2() extends ReduceFunction[(String, Int)] {
    override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
      if (value1._2 > value2._2) {
        (value1._1, value2._2)
      } else {
        (value1._1, value1._2)
      }
    }
  }

  class MyProcessWindowFunc2() extends ProcessWindowFunction[(String, Int), (String, String), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, String)]): Unit = {
      val min = elements.iterator.next().toString()
      out.collect(context.window.getStart.toString, min)
    }
  }


  class MyAggFunc extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {
    override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

    override def add(value: (String, Double), accumulator: (String, Double, Int)): (String, Double, Int) = {
      (value._1, accumulator._2 + value._2, accumulator._3 + 1)
    }

    override def getResult(accumulator: (String, Double, Int)): (String, Double) = {
      val userId = accumulator._1
      val totalAmount = accumulator._2
      val count = accumulator._3
      val avg = if (count > 0) totalAmount / count else 0.0
      (userId, avg)
    }

    override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
}
