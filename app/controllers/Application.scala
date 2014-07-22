package controllers

import play.api._
import play.api.mvc._
import cc.nlplab._
// import play.api.libs.json._
import play.api.libs.json.{JsNull,Json,JsString,JsValue,JsArray}

object Application extends Controller {
  val lgl =new Linggle("hbase-site.xml", "linggle-web1t")

  def index = Action {
    Ok(views.html.index("hi"))
  }

  def query(q: String) = Action {

    val (totalCount, rows) =lgl.query(q)
    // val s = (for(r <- result take 100) yield r.count).sum
    val df = new java.text.DecimalFormat()
    val _jsonRows = for(row <- rows take 100) yield Json.obj(
      "count" -> row.count,
      "phrase" -> row.ngram.zipWithIndex.map{ case (word, i) =>
        if(! row.positions.contains(i)) s"<strong>$word</strong> " else s"$word "
      },
      "count_str" -> df.format(row.count),
      "percent" -> {
        val percent = 100*row.count/totalCount 
        if (percent == 0) " < 1 %" else s"$percent %"
      }
    )
    val jsonRows = JsArray(_jsonRows)


      // Json.toJson(
      // Map(
      //   "count" -> Json.toJson(r.count),
      //   "phrase" -> Json.toJson(
      //     (for(
      //       (word, i) <- r.ngram.zipWithIndex)
      //     yield (
      //       if(r.positions.contains(i)) "<strong>"+word+"</strong>" else word)
      //     ).mkString(" ")),
      //   "count_str" -> Json.toJson(df.format(r.count)),
      //   "percent" -> Json.toJson(
      //     if(100*r.count/totalCount == 0) " < 1 %" else 100*r.count/totalCount +" %")
      // ))
      // val json = Json.toJson(Seq(Json.toJson(Seq(Json.toJson("old"),Json.toJson(rows)))))
    Ok(jsonRows)
  }
}
