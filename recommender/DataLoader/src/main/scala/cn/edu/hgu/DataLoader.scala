package cn.edu.hgu

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress



/**
 * 数据加载
 */
//定义样例类

/**
 * Movie数据集数据模式:
 * 电影编号(mid:Int)^
 * 电影名称(name:String)^
 * 详情描述(descri: String)^
 * 时长(timelong: String)^
 * 发行时间(issue: String)^
 * 拍摄时间(shoot: String)^
 * 语言(language: String)^
 * 类型(genres: String)^
 * 演员信息(actors: String)^
 * 导演(directors: String)^
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String,
                 directors: String)

/**
 * Ratings评分数据集
 * @param uid 用户id
 * @param mid 电影评分
 * @param score 评分
 * @param timestamp 时间戳
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
 * Tags标签数据集
 * @param uid 用户id
 * @param mid 电影评分
 * @param tag 标签
 * @param timestamp 时间戳
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

//把mongodb与ES的配置封装起来成为样例类

/**
 * Mongodb配置文件
 * @param uri Mongodb连接
 * @param db Mongodb数据库
 */
case class MongoConfig(uri:String, db:String)

/**
 * ES配置信息
 * @param httpHosts http主机列表逗号分隔:9200
 * @param transportHosts 集群做内部传输:9300
 * @param index 需要操作的索引
 * @param clustername 集群名称
 */
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clustername:String)
object DataLoader {
  // 以 window 下为例，/resources/movies.csv
  val MOVIE_DATA_PATH = "D:\\zzz\\MovierecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\zzz\\MovierecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\zzz\\MovierecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"


  def main(args: Array[String]): Unit = {
    //定义配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://ha-01:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "192.168.88.150:9200",
      "es.transportHosts" -> "192.168.88.150:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )

    //创建一个sparkConf对象
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //加载数据
    // 在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 将 Movie数据集加载进来
    val movieRDD: RDD[String] = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    //将 MovieRDD 装换为 DataFrame
    val movieDF: DataFrame = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
        attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()
//    println(movieDF.show())
//    println("----------------------------------------我是分割线--------------------------------------")

    //将Rating数据集加载进来
    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    //将 ratingRDD 转换为 DataFrame
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()
//    println(ratingDF.show())
//    println("----------------------------------------我是分割线--------------------------------------")


    //将Tag数据集加载进来
    val tagRDD: RDD[String] = spark.sparkContext.textFile(TAG_DATA_PATH)
    //将 tagRDD 装换为 DataFrame
    val tagDF: DataFrame = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()
//    println(tagDF.show())
//    println("----------------------------------------我是分割线--------------------------------------")
    // 声明一个隐式的配置对象
    implicit val mongoConfig =MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)


    //数据预处理,把movies对应的tag信息添加进去，加一列tag1|tag2|tag3
    import org.apache.spark.sql.functions._
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags")).select("mid","tags")
    // 需要将处理后的 Tag 数据，和 Moive 数据融合，产生新的 Movie 数据
    val movieWithTagsDF = movieDF.join(newTag,Seq("mid","mid"),"left")
    // 声明了一个 ES 配置的隐式参数
    implicit val esConfig = ESConfig(config.get("es.httpHosts").get,
      config.get("es.transportHosts").get,
      config.get("es.index").get,
      config.get("es.cluster.name").get)


    //将数据保存到mongodb
    storeDataInMongoDB(movieDF, ratingDF, tagDF)
    //将数据保存到ES
    storeDataInES(movieWithTagsDF)

    //关闭spark
    spark.stop()


    def storeDataInMongoDB(movieDF: DataFrame, ratingDF:DataFrame, tagDF:DataFrame)(implicit mongoConfig: MongoConfig):Unit={
      //新建一个到 MongoDB 的连接
      val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
      //如果 MongoDB 中有对应的数据库，那么应该删除
      mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
      mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
      mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()
      //将当前数据写入到 MongoDB
      movieDF
        .write
        .option("uri",mongoConfig.uri)
        .option("collection",MONGODB_MOVIE_COLLECTION)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
      ratingDF
        .write
        .option("uri",mongoConfig.uri)
        .option("collection",MONGODB_RATING_COLLECTION)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
      tagDF
        .write
        .option("uri",mongoConfig.uri)
        .option("collection",MONGODB_TAG_COLLECTION)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
      //对数据表建索引
      mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
      mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
      //关闭 MongoDB 的连接
      mongoClient.close()



  }
    def storeDataInES(movieDF:DataFrame)(implicit eSConfig: ESConfig): Unit = {
      //新建一个配置
      val settings:Settings = Settings.builder().put("cluster.name",eSConfig.clustername).build()
      //新建一个 ES 的客户端
      val esClient = new PreBuiltTransportClient(settings)
      //需要将 TransportHosts 添加到 esClient 中
      val REGEX_HOST_PORT = "(.+):(\\d+)".r //主机名.+,\\d正则匹配数字
      eSConfig.transportHosts.split(",").foreach{
        case REGEX_HOST_PORT(host:String,port:String) => {
          esClient.addTransportAddress(new
              InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
        }
      }
      //需要清除掉 ES 中遗留的数据
      if(esClient.admin().indices().exists(new
          IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
        esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
      }
      esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))
      //将数据写入到 ES 中
      movieDF
        .write
        .option("es.nodes",eSConfig.httpHosts)
        .option("es.http.timeout","100m")
        .option("es.mapping.id","mid")
        .mode("overwrite")
        .format("org.elasticsearch.spark.sql")
        .save(eSConfig.index+"/"+ES_MOVIE_INDEX)
    }



  }

}
