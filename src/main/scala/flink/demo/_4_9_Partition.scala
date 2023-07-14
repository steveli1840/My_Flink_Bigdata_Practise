package flink.demo

import org.apache.flink.api.common.functions.{Partitioner, RichMapFunction}
import org.apache.flink.streaming.api.scala._

object _4_9_Partition {

  def testBroadcast(senv: StreamExecutionEnvironment): Unit = {
    senv.setParallelism(3)
    val ds = senv.fromElements(1, 2, 3, 4, 5)
    //分区前
    val ds1 = ds.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    })
    val ds2 = ds1.broadcast
    val ds3 = ds2.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    })
    senv.execute("testBroadcast")
  }

  def testForwardPartition(senv: StreamExecutionEnvironment): Unit = {
    senv.setParallelism(3)
    val ds = senv.fromElements(1, 2, 3, 4, 5)
    //分区前
    val ds1 = ds.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    })
    val ds2 = ds1.forward
    val ds3 = ds2.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    })
    senv.execute("testForwardPartition")
  }

  def testGlobalPartition(senv: StreamExecutionEnvironment): Unit = {
    senv.setParallelism(3)
    val ds = senv.fromElements(1, 2, 3, 4, 5)
    //分区前
    val ds1 = ds.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    })
    val ds2 = ds1.global
    val ds3 = ds2.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    })
    senv.execute("testGlobalPartition")
  }

  def testRebalancedPartition(senv: StreamExecutionEnvironment): Unit = {
    //    senv.setParallelism(2)
    val ds = senv.fromElements(1, 2, 3, 4, 5, 6)
    //分区前
    val ds1 = ds.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    }).setParallelism(2)
    val ds2 = ds1.rebalance
    val ds3 = ds2.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    }).setParallelism(3)
    senv.execute("testRebalancedPartition")
  }

  def testRescalePartition(senv: StreamExecutionEnvironment): Unit = {
    //    senv.setParallelism(2)
    val ds = senv.fromElements(1, 2, 3, 4, 5, 6)
    //分区前
    val ds1 = ds.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    }).setParallelism(4)
    val ds2 = ds1.rescale
    val ds3 = ds2.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    }).setParallelism(2)
    senv.execute("testRebalancedPartition")
  }

  def testShufflePartition(senv: StreamExecutionEnvironment): Unit = {
    //    senv.setParallelism(2)
    val ds = senv.fromElements(1, 2, 3, 4, 5, 6)
    //分区前
    val ds1 = ds.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    }).setParallelism(4)
    val ds2 = ds1.shuffle
    val ds3 = ds2.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        println("元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    }).setParallelism(2)
    senv.execute("testRebalancedPartition")
  }

  def testCustomPartition(senv: StreamExecutionEnvironment): Unit = {
    senv.setParallelism(3)
    val arr = Array("Chinese,98", "Math,88", "English,96")
    val ds = senv.fromCollection(arr)
    val ds1 = ds.map(new RichMapFunction[String, (String, Int)] {
      override def map(value: String): (String, Int) = {
        println("元素值：" + value + "。分区策略前，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        (value.split(",")(0), value.split(",")(1).toInt)
      }
    }).setParallelism(2)

    val ds2 = ds1.partitionCustom(new MyPartitioner, value => value._1)
    ds2.map(new RichMapFunction[(String, Int), (String, Int)] {
      override def map(value: (String, Int)): (String, Int) = {
        println("元素值：" + value + "。分区策略后，子任务编号：" + getRuntimeContext().getIndexOfThisSubtask())
        value
      }
    }).setParallelism(3).print()
    senv.execute("testCustomPartition")
  }

  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //    testBroadcast(senv)
    //    testForwardPartition(senv)
    //    testGlobalPartition(senv)
    //    testRebalancedPartition(senv)
    //    testRescalePartition(senv)
    //    testShufflePartition(senv)
    testCustomPartition(senv)
  }

  class MyPartitioner extends Partitioner[String] {
    override def partition(key: String, numPartitions: Int): Int = {
      if (key.equals("Chinese")) {
        0
      } else if (key.equals("Math")) {
        1
      } else {
        2
      }
    }
  }
}
