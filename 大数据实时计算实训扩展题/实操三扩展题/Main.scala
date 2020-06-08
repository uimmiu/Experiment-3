import java.util.Properties
import java.sql.ResultSet
import java.sql.SQLException

import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import com.bingocloud.util.json.JSONException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.nlpcn.commons.lang.util.IOUtil

import scala.util.parsing.json.{JSONArray, JSONObject}

object Main {
  //s3参数
  val accessKey = "3A2959DC8E8DB2AAC727"
  val secretKey = "W0FGRTU1NjI1OTU2ODE5MzI1QUJFNUU0M0I4Mzk0QTZDRkJEOTQwMkRd"
  val endpoint = "scuts3.depts.bingosoft.net:29999"
  val bucket = "qqs-big-bucket"
  //要读取的文件
  val key = "demo.txt"

  //kafka参数
  val topic = "Stttaatt"
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
    val s3Content = readFile()
    produceToKafka(s3Content)
  }

  /**
   * 从s3中读取文件内容
   *
   * @return s3的文件内容
   */
  def readFile(): String = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    val amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)
    val s3Object = amazonS3.getObject(bucket, key)
    IOUtil.getContent(s3Object.getObjectContent, "UTF-8")
  }


  def getMySqlFile():String={
    import  java.sql.DriverManager
    val url="jdbc:mysql://bigdata28.depts.bingosoft.net:23307/user14_db"
    val properties=new Properties()
    properties.setProperty("DriverClassName","com.mysql.jdbc.Driver")
    properties.setProperty("user","user14")
    properties.setProperty("password","pass@bingo14")
    val connection=DriverManager.getConnection(url,properties)
    val statement=connection.createStatement
    val resultSet=statement.executeQuery("")
    try{
      val res=resultToJson(resultSet)
      resultSet.close()
      res
    } catch {
      case e:Exception=>e.printStackTrace()
        return "0"
    }
  }


  def resultToJson(set: ResultSet):String={
    val array=new JSONArray()
    val metaData=set.getMetaData
    val columnCount=metaData.getColumnCount
    while ({set.next}){
      val JSONObj=new JSONObject()
      for(i<-1 to columnCount){
        val columnName=metaData.getColumnLabel(i)
        val value=set.getString(columnName)
        JSONObj.put(columnName,value)
      }
      array.put(JSONObj)
    }
    var res=""
    for(k<-0 until array.length()){
      res=res+array.get(k).toString()+'\n'
    }
    res
  }
  /**
   * 把数据写入到kafka中
   *
   * @param s3Content 要写入的内容
   */
  def produceToKafka(s3Content: String): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = s3Content.split("\n")
    for (s <- dataArr) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String](topic, null, s)
        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }
}
