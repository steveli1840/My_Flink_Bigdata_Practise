package flink.chapter_01

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputDs = env.readTextFile("src/main/scala/data/words.txt")
    val wordDs = inputDs.flatMap(_.split(" "))
    val tupleDs = wordDs.map((_, 1))
    val groupDs = tupleDs.groupBy(0)
    val resultDs = groupDs.sum(1)
    resultDs.print()
  }
}
