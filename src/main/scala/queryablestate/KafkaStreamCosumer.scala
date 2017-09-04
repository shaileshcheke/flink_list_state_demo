package queryablestate.functions

import org.apache.flink.streaming.api.scala._
import java.util.Properties
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time

object KafkaStreamCosumer{
  
  def main(args: Array[String]) : Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092");
    prop.setProperty("zookeeper.connect", "localhost:2181");
    prop.setProperty("group.id", "test");
    val dataStreamSource = env.
      addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), prop))
    val keyStream = dataStreamSource.map(_.toString)
      .flatMap(new TokenizeFMImplementation())
      .keyBy(0)
      .flatMap(new QueryableListStateFMImple())
      .print
  
    env.execute("KafkaQueryableState")
  }

}