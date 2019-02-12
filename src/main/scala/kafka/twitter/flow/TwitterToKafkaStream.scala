package kafka.twitter.flow

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.log4j.{Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object TwitterToKafkaStream {
  var logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Started Real Time Data Flow")

    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    var acc = ssc.sparkContext.longAccumulator
    val configurationBuilder = new ConfigurationBuilder()
    configurationBuilder.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessSecret)
    val authenticate = new OAuthAuthorization(configurationBuilder.build())
    val stream = TwitterUtils.createStream(ssc, Some(authenticate)).filter(_.getLang() == "en").filter(_.getText.toString.contains("#"))
    stream.foreachRDD { rdd => if(rdd.count()>0){
      acc.add(1.toLong)
      var myvar = acc.value.toLong
      var rddsize = rdd.count().toInt
      //println("acc : "+acc.value+". "+rddsize)

      rdd.foreach { ele =>

        var hashTagEntityArray = ele.getHashtagEntities
        hashTagEntityArray.foreach { hashTag =>
          //if (isAboutApple(hashTag.getText)) {
          val formatedDate = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",Locale.ENGLISH)
            .parse(ele.getCreatedAt.toString))
          println("ele "+acc.value)
          logger.info("Sending message to KafkaProducer")
          KafkaProducerRaw.sendRecordToKafka(acc.value+". "+rddsize+". "+myvar+". "+formatedDate, hashTag.getText, ele.getText.replaceAll("\n", " "))
            logger.info(s"RawTweet : $ele HashTag: ${hashTag.getText} Text : ${ele.getText}")
            // println("abcd created at "+ele.getCreatedAt + " Current time "+ LocalDateTime.now())
            // println("Parsed date : "+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss ").format(new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy",Locale.ENGLISH)
            //.parse(ele.getCreatedAt.toString)))
          //}
        }

      }


    }
    }

    def isAboutApple(hashTag: String): Boolean = {
      var list = List[String]("apple", "iphone", "ipad", "applewatch", "ipod", "ios", "ilife")
      if (list.contains(hashTag.toLowerCase()))
        true
      else false
    }

    ssc.start()
    ssc.awaitTermination()
  }

}