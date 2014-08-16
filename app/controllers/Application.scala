package controllers

import play.api._
import play.api.mvc._
import models._

import play.api.Logger

// import cc.nlplab._
// import play.api.libs.json._

import play.api.libs.json.{JsNull,Json,JsString,JsValue,JsArray}

object Application extends Controller {
  val lgl =new Linggle("hbase-site.xml", "linggle-web1t")
  val df = new java.text.DecimalFormat()

  def index = Action { Ok(views.html.index("hi"))}
  
  def query(q: String) = Action {
    
    val (totalCount, rows) = {
      val (_totalCount, _rows) = lgl.get(q.replace("@", "/"), ParseMode.Fast)
      if ((_rows take 50).size == 50)
        (_totalCount, _rows)
      else {
        Logger.info("Fast Mode Failed, switch to POS Partially Expanded Mode")
        lgl.get(q.replace("@", "/"), ParseMode.PosPartiallyExpanded)}}

    val _totalCount = (for(row <- rows take 50) yield row.count).sum
    val jsonRows = (
      for(row <- rows take 50) yield Json.arr(Json.obj(
        "count" -> row.count,
        "phrase" -> row.ngram.zipWithIndex.map { case (word, i) =>
          if(! row.positions.contains(i)) s"<strong>$word</strong> " else s"$word "},
        "count_str" -> df.format(row.count),
        "percent" -> {
          val percent = 100*row.count/_totalCount
          if (percent == 0) " < 1 %" else s"$percent %"})))
    Ok(
      jsonRows match {
        case Stream.Empty => new JsArray
        case jsonStream: Stream[JsArray] => jsonStream reduce (_ ++ _)})

  }
}
