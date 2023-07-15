package flink.chapter_04.c08_serialization

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestCase {
  case class WordCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(
      WordCount("hello", 1),
      WordCount("word", 2)
    )
    val ds2 = ds1.keyBy(_.word)
    ds2.print()
    env.execute("Test case class")
  }
}
