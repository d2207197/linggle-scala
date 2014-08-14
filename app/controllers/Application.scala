package controllers

import play.api._
import play.api.mvc._
import models._

// import cc.nlplab._
// import play.api.libs.json._

import play.api.libs.json.{JsNull,Json,JsString,JsValue,JsArray}

object Application extends Controller {
  val lgl =new Linggle("hbase-site.xml", "linggle-web1t")
  val df = new java.text.DecimalFormat()

  def index = Action { Ok(views.html.index("hi"))}
  
  def query(q: String) = Action {
    
    val (totalCount, rows) = lgl get q.replace("@", "/")
    // val s = (for(r <- result take 100) yield r.count).sum
    val jsonRows = (
      for(row <- rows take 100) yield Json.arr(Json.obj(
        "count" -> row.count,
        "phrase" -> row.ngram.zipWithIndex.map { case (word, i) =>
          if(! row.positions.contains(i)) s"<strong>$word</strong> " else s"$word "},
        "count_str" -> df.format(row.count),
        "percent" -> {
          val percent = 100*row.count/totalCount
          if (percent == 0) " < 1 %" else s"$percent %"})))
    Ok(
      jsonRows match {
        case Stream.Empty => new JsArray
        case jsonStream: Stream[JsArray] => jsonStream reduce (_ ++ _)})
  }
}
