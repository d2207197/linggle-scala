package models

import com.mongodb.casbah.Imports._

// import com.mongodb.DBObject
import org.bson.types.ObjectId

import com.mongodb.casbah.commons.conversions.scala._

case class PantelSimWords (
  id: ObjectId = new ObjectId(),
  word: String,
  partOfSpeech: String,
  simWords: Map[String, Double]
)

object PantelSimWords {
  // RegisterJodaTimeConversionHelpers()

  val pantelMongo = MongoClient("lost.nlpweb.org", 27017)("similarity")("pantel")
  def get(word: String, bound: Double , topN: Int): Option[PantelSimWords] = {
    pantelMongo.findOne( "word" $eq word) map { bson =>
      fromBson(bson, bound, topN)
    }
  }

  def fromBson(o: DBObject, bound: Double, topN: Int): PantelSimWords = PantelSimWords (
    simWords = {
      val dbObjects: List[DBObject] = o.getAs[List[DBObject]]("simWords") getOrElse List[DBObject]()
      dbObjects
        .sortBy{ x=>
        - x.as[Double]("score")
      }
        .take(topN)
        .foldLeft(Map[String,Double]()) {
        case (m, x) if x.as[Double]("score") > bound  => {
          m + (x.as[String]("simword")  -> x.as[Double]("score") )
        }
        case (m, x) => m
      }
    },
    // map {x => SimWord(x.as[String]("simword"), x.as[Double]("score"))}),
    word =  o.getAs[String]("word") getOrElse "",
    partOfSpeech = o.getAs[String]("POS") getOrElse "",
    id = o.as[ObjectId]("_id")
  )

}

