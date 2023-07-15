package flink.chapter_04.c06_transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 需要注意的是，union()算子要求多个数据流的数据类型必须相同。
 */
object TestUnion {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(
      (0, 0, 0),
      (1, 1, 1),
      (2, 2, 2)
    )
    val ds2 = env.fromElements(
      (4, 4, 4),
      (5, 5, 5),
      (6, 6, 6)
    )
    val ds3 = ds1.union(ds2)
    ds3.print()
    env.execute("TestUnion")
  }

}
