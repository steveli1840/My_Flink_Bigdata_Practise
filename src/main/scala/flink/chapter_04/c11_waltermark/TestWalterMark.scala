package flink.chapter_04.c11_waltermark

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration


object TestWalterMark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9999)
    env.getConfig.setAutoWatermarkInterval(200)
    env.setParallelism(1)

    val carStream = stream.map(l => {
      val arr = l.split(",")
      CarData(arr(0), arr(1).toInt, arr(2).toLong)
    })

    val walterCarStream = carStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[CarData](Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[CarData] {
        override def extractTimestamp(element: CarData, recordTimestamp: Long): Long = element.time
      })
    )

    walterCarStream.keyBy(_.id).window(TumblingEventTimeWindows.of(Time.seconds(5))).reduce(new MyReduceFunc3, new MyProcessWindowFunc3).print()

    env.execute("WaltermarkExample")
  }

  /**
   * 车辆数据
   *
   * @param id    信号灯id
   * @param count 通过的车辆数量
   * @param time  事件的时间戳
   */
  case class CarData(id: String, count: Int, time: Long)

  /**
   * 使用增量聚合函数进行预聚合,累加同一个信号灯下的车辆数量
   */
  class MyReduceFunc3 extends ReduceFunction[CarData] {
    //CarData(信号灯ID，车辆总数，事件时间戳)，事件时间戳取任意值都可，此处只是占位使用
    override def reduce(value1: CarData, value2: CarData): CarData = CarData(value1.id, value1.count + value2.count, value1.time)
  }

  //使用全量聚合函数获取窗口开始和结束时间
  //ReduceFunction计算完毕后，会将每组数据（以Key分组）的计算结果输入到ProcessWindowFunction的process()方法中
  class MyProcessWindowFunc3 extends ProcessWindowFunction[CarData, String, String, TimeWindow] {
    //窗口结束的时候调用（每次传入一个分组的数据）
    override def process(key: String, context: Context, elements: Iterable[CarData], out: Collector[String]): Unit = {
      //从elements变量中获取聚合结果，本例该变量中只有一条数据,即聚合的总数
      val carDataReduce = elements.iterator.next()
      //输出窗口开始时间与最小值
      out.collect("窗口[" + context.window.getStart.toString + "~" + context.window.getEnd + ")的计算结果：" + (key, carDataReduce.count))
    }
  }
}

//test data
/*

1001,3,1000
1001,2,2000
1002,2,2000
1002,3,3000
1001,5,5000
1001,3,8000
1001,2,4000
1001,3,12000
1001,2,3000
1001,2,4000

 */
