package flink.chapter_04.c18_practise

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object StreamKafkaWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()

    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "test")
    val stream = env.addSource(new FlinkKafkaConsumer[String]("topictest", new SimpleStringSchema(), props))

    val result = stream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(_._1).sum(1)
    result.print()

    env.execute("StreamKafkaWordCount")
  }
}
