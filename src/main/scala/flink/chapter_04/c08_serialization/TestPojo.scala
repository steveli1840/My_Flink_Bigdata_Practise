package flink.chapter_04.c08_serialization

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestPojo {
  class WordWithCount(var word: String, var count: Int) {
    def this() {
      this(null, -1)
    }

    override def toString: String = {
      "word:" + word + ",count:" + count
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(
      new WordWithCount("hello", 1),
      new WordWithCount("word", 2)
    )
    val ds2 = ds1.keyBy(_.word)
    ds2.print()
    env.execute("Test Pojo")
  }
}
