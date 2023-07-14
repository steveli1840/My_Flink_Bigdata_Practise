package flink.demo

import org.apache.flink.api.scala._

object WorldCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputDataSet:DataSet[String] = env.readTextFile("src/main/scala/data/words.txt")
    val wordDataSet: DataSet[String] = inputDataSet.flatMap(_.split(" "))
    val tupleDataSet: DataSet[(String,Int)] = wordDataSet.map((_, 1))
    val groupDataSet = tupleDataSet.groupBy(0)
    val resultDataSet = groupDataSet.sum(1)
    resultDataSet.print()
  }

}
