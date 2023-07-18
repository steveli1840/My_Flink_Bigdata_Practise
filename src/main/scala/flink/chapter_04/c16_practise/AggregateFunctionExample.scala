package flink.chapter_04.c16_practise

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9999)
    stream.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toDouble)
    }).keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
      .aggregate(new MyAggFunction)
      .print("output")

    env.execute("AggregateFunctionExample")
  }

}


class MyAggFunction extends AggregateFunction[(String, Double), (Int, Double), Double] {
  override def createAccumulator(): (Int, Double) = (0, 0)

  override def add(value: (String, Double), accumulator: (Int, Double)): (Int, Double) = {
    (accumulator._1 + 1, accumulator._2 + value._2)
  }

  override def getResult(accumulator: (Int, Double)): Double = {
    (accumulator._2 / accumulator._1).formatted("%.2f").toDouble
  }

  override def merge(a: (Int, Double), b: (Int, Double)): (Int, Double) = {
    (a._1 + b._1, a._2 + b._2)
  }
}
