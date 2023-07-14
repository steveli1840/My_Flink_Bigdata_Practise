package flink.demo

import org.apache.flink.streaming.api.scala._

object _4_8_Serialization {

  case class WordCount(word: String, count: Int)

  class WordWithCount(var word: String, var count: Int) {
    def this() {
      this(null, -1)
    }
  }

  def TestCase(senv: StreamExecutionEnvironment): Unit = {
    val input = senv.fromElements(WordCount("hello", 1), WordCount("world", 1))
    val kds = input.keyBy(_.word)
    kds.print()
    senv.execute("TestCase")
  }

  def TestWordWithCount(senv: StreamExecutionEnvironment): Unit = {
    val input = senv.fromElements(new WordWithCount("hello", 1), new WordWithCount("world", 2))
    val kds = input.keyBy(_.word)
    kds.print()
    senv.execute("TestWordWithCount")
  }

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    TestCase(senv)
    TestWordWithCount(senv)
  }

}
