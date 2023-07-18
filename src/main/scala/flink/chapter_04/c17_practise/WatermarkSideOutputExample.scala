package flink.chapter_04.c17_practise

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * 案例分析：计算5秒内每个信号灯通过的汽车数量
 */
object WatermarkSideOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9999)
    env.getConfig.setAutoWatermarkInterval(200)
    env.setParallelism(1)
    val carStream = stream.map(line => {
      val arr = line.split(",")
      CarData(arr(0), arr(1).toInt, arr(2).toLong)
    })

    val waterCarStream = carStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[CarData](Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[CarData] {
          override def extractTimestamp(element: CarData, recordTimestamp: Long): Long = {
            element.eventTime
          }
        })
    )

    val lateOutputTag = new OutputTag[CarData]("late-data")

    val resultStream = waterCarStream.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .allowedLateness(Time.seconds(3))
      .sideOutputLateData(lateOutputTag)
      .reduce(new MyReduceFunction4, new MyProcessWindowFunction4)

    resultStream.print("主数据流")
    val sideOutputStream = resultStream.getSideOutput(lateOutputTag)
    sideOutputStream.print("侧道输出流")
    env.execute("WatermarkSideOutputExample")
  }
}


/**
 *
 * @param id
 * @param count
 * @param eventTime
 */
case class CarData(id: String, count: Int, eventTime: Long)

class MyReduceFunction4 extends ReduceFunction[CarData] {
  override def reduce(value1: CarData, value2: CarData): CarData = CarData(value1.id, value1.count + value2.count, value1.eventTime)
}

class MyProcessWindowFunction4 extends ProcessWindowFunction[CarData, String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[CarData], out: Collector[String]): Unit = {
    val carDataReduce = elements.iterator.next()
    out.collect("窗口【" + context.window.getStart.toString + "~" + context.window.getEnd.toString + ")的计算结果" + (key, carDataReduce.count))
  }
}