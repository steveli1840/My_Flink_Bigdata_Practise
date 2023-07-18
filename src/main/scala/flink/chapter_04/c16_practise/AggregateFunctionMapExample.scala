package flink.chapter_04.c16_practise

import org.apache.flink.api.common.functions.RichAggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateFunctionMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9999)
    stream.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toDouble)
    }).keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(new MyAggMapFunction)
      .print("output")

    env.execute("AggregateFunctionExample")
  }
}

class MyAggMapFunction extends RichAggregateFunction[(String, Double), MapState[String, (Int, Double)], Map[String, Double]] {

  override def createAccumulator(): MapState[String, (Int, Double)] = {
    val descriptor = new MapStateDescriptor[String, (Int, Double)]("myMapState", classOf[String], classOf[(Int, Double)])
    getRuntimeContext.getMapState(descriptor)
  }

  override def add(value: (String, Double), accumulator: MapState[String, (Int, Double)]): Unit = {
    val userId = value._1
    val amount = value._2

    val currentCountSum = Option(accumulator.get(userId)).getOrElse((0, 0.0))
    val updatedCountSum = (currentCountSum._1 + 1, currentCountSum._2 + amount)

    accumulator.put(userId, updatedCountSum)
  }

  override def getResult(accumulator: MapState[String, (Int, Double)]): Map[String, Double] = {
    val resultMap = collection.mutable.Map[String, Double]()

    val iterator = accumulator.entries().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val userId = entry.getKey
      val (count, sum) = entry.getValue
      val avg = sum / count
      resultMap(userId) = avg
    }

    resultMap.toMap
  }

  override def merge(a: MapState[String, (Int, Double)], b: MapState[String, (Int, Double)]): Unit = {
    val iterator = b.entries().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val userId = entry.getKey
      val countSum = entry.getValue
      a.put(userId, countSum)
    }
  }
}

//尝试改造，但是。。。
//报错的。。。。，有时间修复