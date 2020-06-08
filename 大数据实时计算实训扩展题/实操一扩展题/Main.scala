import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Main {
  val target="b"
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Linux or Mac:nc -l 9999
    //Windows:nc -l -p 9999
    val text = env.socketTextStream("localhost", 9999)
    val stream = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.contains(target)
      }
    }.map(_.count(x=>(x.toString.equals(target))))
        .timeWindowAll(Time.minutes(1),Time.seconds(5))
        .sum(0)
        .map("过去一分钟b出现了"+_+"次")
    stream.print()
    env.execute("Window Stream WordCount")
  }

}
