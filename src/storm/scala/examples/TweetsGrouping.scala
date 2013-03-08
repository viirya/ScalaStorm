package storm.scala.examples

import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}
import collection.mutable.{Map, HashMap}
import util.Random
import com.mongodb.casbah.Imports._


class TweetStreamSpout extends StormSpout(outputFields = List("geo_lat", "geo_lng", "lat", "lng")) {

  var mongoClient: com.mongodb.casbah.MongoClient = _
  var mongoDB: com.mongodb.casbah.MongoDB = _
  var mongoColl: com.mongodb.casbah.MongoCollection = _
  var mongoCursor: com.mongodb.casbah.MongoCursor = _

  setup {
    mongoClient =  MongoClient("")
    mongoDB = mongoClient("")
    mongoDB.authenticate("", "")
    mongoColl = mongoDB("")
    mongoCursor = mongoColl.find()
  }

  def parse(item: com.mongodb.DBObject): (Option[Double], Option[Double]) = {

    var lat: Double = 0.0
    var lng: Double = 0.0
 
    if (item.get("geo") != null) {

      try {
        lat = item("geo").asInstanceOf[BasicDBObject].get("coordinates").asInstanceOf[BasicDBList].get(0).asInstanceOf[Double]
      } catch {
        case _ => lat = item("geo").asInstanceOf[BasicDBObject].get("coordinates").asInstanceOf[BasicDBList].get(0).asInstanceOf[Int].toDouble
      }
 
      try {
        lng = item("geo").asInstanceOf[BasicDBObject].get("coordinates").asInstanceOf[BasicDBList].get(1).asInstanceOf[Double]
      } catch {
        case _ => lng = item("geo").asInstanceOf[BasicDBObject].get("coordinates").asInstanceOf[BasicDBList].get(1).asInstanceOf[Int].toDouble
 
      }
      return (Some(lat), Some(lng))
 
    } else {
        return (None, None)
    }
  }
        
 
  def nextTuple = {
    Thread sleep 100
    if (mongoCursor.hasNext) {
      var item = mongoCursor.next()
      parse(item) match {
        case (Some(lat: Double), Some(lng: Double)) => emit (math.floor(lat * 100000), math.floor(lng * 100000), lat, lng)
        case (_, _) =>
      }
    }
  }
}


class GeoGrouping extends StormBolt(List("geo_lat", "geo_lng", "lat", "lng")) {
  var average_lat: Map[String, Double] = _
  var average_lng: Map[String, Double] = _
 
  setup {
    average_lat = new HashMap[String, Double]().withDefaultValue(0.0)
    average_lng = new HashMap[String, Double]().withDefaultValue(0.0)
  }
  def execute(t: Tuple) = t matchSeq {
    case Seq(geo_lat: Double, geo_lng: Double, lat: Double, lng: Double) =>
      average_lat(geo_lat.toString() + geo_lng.toString()) += lat
      average_lng(geo_lat.toString() + geo_lng.toString()) += lng

      average_lat(geo_lat.toString() + geo_lng.toString()) /= 2.0
      average_lng(geo_lat.toString() + geo_lng.toString()) /= 2.0
 
      using anchor t emit (geo_lat, geo_lng, average_lat(geo_lat.toString() + geo_lng.toString()), average_lng(geo_lat.toString() + geo_lng.toString()))
      t ack
  }
}


object TweetsGroupingTopology {
  def main(args: Array[String]) = {
    val builder = new TopologyBuilder

    builder.setSpout("tweetstream", new TweetStreamSpout, 1)
    builder.setBolt("geogrouping", new GeoGrouping, 12)
        .fieldsGrouping("tweetstream", new Fields("geo_lat", "geo_lng"))

    val conf = new Config
    conf.setDebug(true)
    conf.setMaxTaskParallelism(3)

    val cluster = new LocalCluster
    cluster.submitTopology("tweets-grouping", conf, builder.createTopology)
    //Thread sleep 10000
    //cluster.shutdown
  }
}
