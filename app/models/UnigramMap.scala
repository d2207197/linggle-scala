package cc.nlplab
import com.fasterxml.jackson.databind.ObjectMapper 
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


// class UnigramMap(toIdxMap:Map[String, Int], fromIdxMap:Map[Int, String]) {
//   def apply(unigram:String) = toIdxMap(unigram)
//   def apply(count:Int) = fromIdxMap(count)
//   def contains(unigram:String) = toIdxMap contains unigram
//   def contains(count:Int) = fromIdxMap contains count
// }

// object UnigramMap {
//   def apply(jsonPath: String) = {
//     val mapper = new ObjectMapper() with ScalaObjectMapper
//     mapper.registerModule(DefaultScalaModule)
//     val _to:Map[String, Int] =  mapper.readValue[Map[String,Int]](new java.io.File (jsonPath))
//     val _from = _to map {_.swap}
//     new UnigramMap(_to, _from)
//   }
// }

class POS(posMap: Map[String, Vector[String]]) {
  def apply(unigram: String) = posMap(unigram)
  def apply(unigram: String, pos: String) =
    if (posMap contains unigram)
      posMap(unigram) contains pos
    else false
}

object POS {
  def apply(jsonIS: java.io.InputStream) = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val posMap:Map[String, Vector[String]] =  mapper.readValue[Map[String,Vector[String]]](jsonIS)
    new POS(posMap)
  }

  def apply(jsonFile: java.io.File) = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val posMap:Map[String, Vector[String]] =  mapper.readValue[Map[String,Vector[String]]](jsonFile)
    new POS(posMap)
  }

}

