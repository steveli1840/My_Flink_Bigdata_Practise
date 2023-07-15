package flink.chapter_04.c06_transformation

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestReduce2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(Score("Zhang", "English", 98), Score("Li", "English", 88), Score("Zhang", "Math", 78), Score("Li", "Match", 68))
    ds1.keyBy(_.name).reduce(new MyReduceFunc).print()
    env.execute("Test Reduce2")
  }

}

case class Score(name: String, course: String, score: Int)

class MyReduceFunc() extends ReduceFunction[Score] {
  override def reduce(value1: Score, value2: Score): Score = {
    Score(value1.name, "Sum", value1.score + value2.score)
  }
}
