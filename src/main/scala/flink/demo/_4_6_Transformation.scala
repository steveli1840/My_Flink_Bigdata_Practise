package flink.demo

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, KeyedStream, StreamExecutionEnvironment}

object _4_6_Transformation {

  private def TestMap(env: ExecutionEnvironment) = {
    val ds1 = env.fromCollection(List(1, 2, 3, 4, 5, 6))
    val ds2 = ds1.map(_ + 1)
    ds2.print()
    env.execute("MyMap")
  }

  private def TestFlatMap(env: ExecutionEnvironment) = {
    val ds1 = env.fromCollection(List("hello java hive", "flink hello"))
    val ds2 = ds1.flatMap(_.split(" "))
    ds2.print()
    env.execute("MyFlatMap")
  }

  private def TestFilter(env: ExecutionEnvironment) = {
    val ds1 = env.fromCollection(List(1, 2, 3, 4, 5, 6))
    val ds2 = ds1.filter(_ > 3)
    ds2.print()
    env.execute("MyFilter")
  }

  private def TestKeyBy(senv: StreamExecutionEnvironment) = {
    val ds1 = senv.fromCollection(List(("zhangsan", 98), ("lisi", 88), ("zhangsan", 78), ("lisi", 68), ("wangwu", 55), ("wangwu", 44)))
    val kds1: KeyedStream[(String, Int), String] = ds1.keyBy(_._1)
    kds1.print()
    senv.execute("MyKeyBy")
  }

  private def TestReduce(senv: StreamExecutionEnvironment) = {
    val ds1 = senv.fromCollection(List(("zhangsan", 98), ("lisi", 88), ("zhangsan", 78), ("lisi", 68), ("wangwu", 55), ("wangwu", 44)))
    val rds = ds1.keyBy(_._1).reduce((t1, t2) => {
      (t1._1, t1._2 + t2._2)
    })
    rds.print()
    senv.execute("MyReduct")
  }

  private def TestReductFunc(senv: StreamExecutionEnvironment) = {
    val ds1 = senv.fromCollection(List(("zhangsan", 98), ("lisi", 88), ("zhangsan", 78), ("lisi", 68), ("wangwu", 55), ("wangwu", 44)))
    val rds = ds1.keyBy(_._1).reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1, value1._2 + value2._2)
      }
    })
    rds.print()
    senv.execute("MyReduce")
  }

  private def TestReduceCaseClass(senv: StreamExecutionEnvironment) = {
    val ds = senv.fromElements(Score("Zhang", "English", 98), Score("Li", "English", 88), Score("Zhang", "Math", 78), Score("Li", "Math", 79))
    val rds = ds.keyBy(_.name).reduce(new MyReduceFunc)
    rds.print()
    senv.execute("ReduceCaseClass")
  }

  private def TestAggSum(senv: StreamExecutionEnvironment) = {
    val tds = senv.fromElements(
      (0, 0, 0),
      (0, 1, 1),
      (0, 2, 2),
      (1, 0, 6),
      (1, 1, 7),
      (1, 2, 8)
    )
    val sumDs = tds.keyBy(_._1).sum(1)
    sumDs.print()
    senv.execute("MyAggSum")
  }

  private def TestAggMax(senv: StreamExecutionEnvironment) = {
    val tds = senv.fromElements(
      (0, 0, 0),
      (0, 1, 1),
      (0, 2, 2),
      (1, 0, 6),
      (1, 1, 7),
      (1, 2, 8)
    )
    val maxDs = tds.keyBy(_._1).max(1)
    maxDs.print()
    senv.execute("MyAggMax")
  }

  private def TestAggMaxBy(senv: StreamExecutionEnvironment) = {
    val tds = senv.fromElements(
      (0, 0, 0),
      (0, 1, 1),
      (0, 2, 2),
      (1, 0, 6),
      (1, 1, 7),
      (1, 2, 8)
    )
    val maxByDs = tds.keyBy(_._1).maxBy(1)
    maxByDs.print()
    senv.execute("MyAggMaxBy")
  }

  private def TestUnion(senv: StreamExecutionEnvironment) = {
    val ds1 = senv.fromElements(
      (0, 0, 0),
      (1, 1, 1),
      (2, 2, 2)
    )
    val ds2 = senv.fromElements(
      (3, 3, 3),
      (4, 4, 5),
      (5, 5, 5)
    )
    val unionDs = ds1.union(ds2)
    unionDs.print()
    senv.execute("MyUnion")
  }

  def TestConnect(senv: StreamExecutionEnvironment) = {
    val ds1 = senv.fromElements(1, 2, 5, 3)
    val ds2 = senv.fromElements("a", "b", "c", "d")
    val connDs: ConnectedStreams[Int, String] = ds1.connect(ds2)
    val rds = connDs.map(new MyCoMapFunc)
    rds.print()
    senv.execute("MyConnect")
  }

  def main(args: Array[String]): Unit = {
    //    val env = ExecutionEnvironment.getExecutionEnvironment
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //    TestMap(env)
    //    TestFlatMap(env)
    //    TestFilter(env)
    //    TestKeyBy(senv)
    //    TestReduce(senv)
    //    TestReductFunc(senv)
    //    TestReduceCaseClass(senv)
    //    TestAggSum(senv)
    //    TestAggMax(senv)
    //    TestAggMaxBy(senv)
    //    TestUnion(senv)
    TestConnect(senv)

  }


  class MyReduceFunc() extends ReduceFunction[Score] {
    override def reduce(value1: Score, value2: Score): Score = {
      Score(value1.name, "Sum", value1.score + value2.score)
    }
  }

  class MyCoMapFunc extends CoMapFunction[Int, String, String] {
    override def map1(value: Int): String = value.toString.toUpperCase()

    override def map2(value: String): String = value.toUpperCase()
  }
}

case class Score(name: String, course: String, score: Int)
