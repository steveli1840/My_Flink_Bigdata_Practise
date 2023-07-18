package flink.chapter_04.c19_practise

import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.text.SimpleDateFormat
import java.util.{Comparator, PriorityQueue, Random}
import java.util.concurrent.TimeUnit

object Tmall {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new MyDataSource)
    val initResultDs = stream.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
      .aggregate(new MyPriceAggFunc, new MyWindowFunc)
    //    initResultDs.print("分类销售总额")

    initResultDs.keyBy(_.dateTime)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .process(new MyFinalProcessWindowFunction())
    env.execute("Tmall")
  }
}

class MyDataSource extends RichSourceFunction[(String, Double)] {
  private var isRunning = true

  private val categories = Array(
    "女装",
    "男装",
    "图书",
    "家电",
    "洗护",
    "美妆",
    "运动",
    "游戏",
    "户外",
    "家具",
    "乐器",
    "办公"
  )

  override def run(ctx: SourceFunction.SourceContext[(String, Double)]): Unit = {
    val random = new Random()
    while (isRunning) {
      val index = random.nextInt(categories.length)
      val category = categories(index)
      val price = random.nextDouble() * 100
      val orderPrice = price.formatted("%.2f").toDouble
      TimeUnit.MICROSECONDS.sleep(20)
      ctx.collect((category, orderPrice))
    }
  }

  override def cancel(): Unit = isRunning = false
}

/**
 * 自定义订单金额聚合函数,对每一个分类的所有订单金额求和
 * AggregateFunction<输入元素类型, 累加器（中间聚合状态）类型, 输出元素类型>
 */
class MyPriceAggFunc extends AggregateFunction[(String, Double), Double, Double] {
  /**
   * 创建累加器，初始值为0
   *
   * @return
   */
  override def createAccumulator(): Double = 0D

  /**
   * 将窗口中的元素添加到累加器
   *
   * @param value       窗口中的元素
   * @param accumulator 累加器：金额总和
   * @return 累加器
   */
  override def add(value: (String, Double), accumulator: Double): Double = {
    value._2 + accumulator
  }

  /**
   * 获取累加结果：金额总和
   *
   * @param accumulator 累加器
   * @return 累加器：金额总和
   */
  override def getResult(accumulator: Double): Double = accumulator

  /**
   *
   * 合并累加器，只有会话窗口才使用
   *
   * @param a 要合并的累加器
   * @param b 要合并的另一个累加器
   * @return 新累加器
   */
  override def merge(a: Double, b: Double): Double = a + b
}

/**
 * 自定义窗口函数WindowFunction,实现收集结果数据
 * WindowFunction[输入元素类型, 输出元素类型, Key类型, 窗口]
 */
class MyWindowFunc extends WindowFunction[Double, CategoryPojo, String, TimeWindow] {

  /**
   * MyPriceAggregateFunction的聚合结果将输入到该方法，结果有多个Key，因此每个Key调用一次
   *
   * @param key    分类名称，即MyPriceAggregate聚合对应的每一组的Key
   * @param window 时间窗口
   * @param input  上一个函数（MyPriceAggregateFunction）每一个Key对应的聚合结果
   * @param out    收集器，收集记录并发射出去
   */
  override def apply(key: String, window: TimeWindow, input: Iterable[Double], out: Collector[CategoryPojo]): Unit = {
    val totalPrice: Double = input.iterator.next
    val roundPrice = totalPrice.formatted("%.2f").toDouble
    val currentTimeMillis = System.currentTimeMillis
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dataTime = df.format(currentTimeMillis)
    out.collect(CategoryPojo(key, roundPrice, dataTime))
  }
}

/**
 *
 * 存储聚合的结果
 *
 * @param category   分类名称
 * @param totalPrice 该分类总销售额
 * @param dateTime   系统对该分类的统计时间
 */
case class CategoryPojo(category: String, totalPrice: Double, dateTime: String)

/**
 * 定义全量聚合窗口处理类,收集结果数据
 * ProcessWindowFunction[输入值类型, 输出值类型, Key类型, 窗口]
 */
class MyFinalProcessWindowFunction() extends ProcessWindowFunction[CategoryPojo, Object, String, TimeWindow] {
  /**
   * 窗口结束的时候调用（每次传入一个分组的数据）
   *
   * @param key      CategoryPojo中的dataTime
   * @param context  窗口上下文
   * @param elements 某个Key对应的CategoryPojo集合
   * @param out      收集计算结果并输出
   */
  override def process(key: String, context: Context, elements: Iterable[CategoryPojo], out: Collector[Object]): Unit = {
    var allTotalPrice: Double = 0D
    //PriorityQueue（优先队列），一个基于优先级堆的无界优先级队列
    //实现一个容量为3的小顶堆，其元素按照CategoryPojo的totalPrice升序排列
    val queue = new PriorityQueue[CategoryPojo](3, new Comparator[CategoryPojo] {
      override def compare(o1: CategoryPojo, o2: CategoryPojo): Int = {
        if (o1.totalPrice >= o2.totalPrice) {
          1
        } else {
          -1
        }
      }
    })

    //1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额
    for (elem <- elements) {
      //把之前聚合的各个分类的销售额加起来，就是全站的总销量额
      val price = elem.totalPrice //某个分类的总销售额
      allTotalPrice += price

      //2.计算出各个分类的销售额top3,其实就是对各个分类的销售额进行排序取前3
      if (queue.size() < 3) { //小顶堆size<3,说明数不够,直接放入
        queue.add(elem)
      } else { //小顶堆size=3,说明小顶堆满了,进来的新元素需要与堆顶元素比较
        val top: CategoryPojo = queue.peek() //得到顶上的元素
        if (elem.totalPrice > top.totalPrice) {
          queue.poll //移除堆顶的元素，或者queue.remove(top);
          queue.add(elem) //添加新元素，会进行升序排列
        }
      }
    }

    //对queue中的数据降序排列（原来是升序，改为降序）
    val pojoes = queue.stream().sorted(new Comparator[CategoryPojo] {
      override def compare(o1: CategoryPojo, o2: CategoryPojo): Int = {
        if (o1.totalPrice >= o2.totalPrice) {
          -1
        } else {
          1
        }
      }
    })

    println("时间：" + key +
      "总交易额:" + allTotalPrice.formatted("%.2f") +
      "\ntop3分类:\n" + StringUtils.join(pojoes.toArray, "\n"))
    println("-------------")
  }
}