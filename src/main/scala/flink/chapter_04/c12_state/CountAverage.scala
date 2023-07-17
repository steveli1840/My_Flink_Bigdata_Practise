package flink.chapter_04.c12_state

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object CountAverage {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(List(
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L)
    )).keyBy(_._1).flatMap(new CountAverage()).print()
    env.execute("ExampleKeyedState")
  }
}

/**
 * 自定义flatmap处理函数
 */
class CountAverage() extends RichFlatMapFunction[(Long, Long), (Long, Float)] {

  //1.声明状体对象ValueState. 用来存放状态数据
  private var sum: ValueState[(Long, Float)] = _

  //2.状态初始化，初始化调用（默认生命周期方法）
  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      //创建状态描述器
      new ValueStateDescriptor[(Long, Float)]("average", createTypeInformation[(Long, Float)])
    )
  }

  override def flatMap(input: (Long, Long), out: Collector[(Long, Float)]): Unit = {
    val tmpCurrentSum = sum.value()
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0.0F)
    }
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    sum.update(newSum)

    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }
}
