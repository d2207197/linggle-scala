package models

import play.api.Play.current
import play.api.{Logger,Play}
// import    play.Logger
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get,Scan,ResultScanner,Result}
import org.apache.hadoop.conf.Configuration
import java.io.FileInputStream
import java.io.InputStream

import com.mongodb.casbah.Imports._


import org.apache.hadoop.hbase.util.{Bytes, Writables}

import org.apache.hadoop.hbase.filter.PageFilter


case class LinggleQuery(terms: Vector[String] , length: Int , positions: Vector[Int], filters: Vector[Tuple2[Int, String]])
{
  override def toString = "LQ(ts: \"%s\", l: %d, ps: %s, fs: [%s])" format
  (terms.mkString(" "), length, positions.mkString(""), filters.mkString(", "))
}


object LinggleQuery {


  trait Atom

  trait HereAtom extends Atom

  case class Or(terms: List[NonWildCard]) extends HereAtom {
    override def toString = terms.mkString("Or(", ", ", ")")
  }
  case object WildCard extends HereAtom
  trait NonWildCard extends HereAtom

  case class POS(pos: String) extends NonWildCard
  case class Term(term: String) extends NonWildCard
  case class SimWords(term: String, bound: Double, topN: Int) extends NonWildCard

  case class Maybe(expr: HereAtom) extends Atom
  case object AnyWildCard extends Atom


  import scala.util.parsing.combinator._

  object QueryParser extends JavaTokenParsers {
    val wildCard  = "_".r ^^^ { WildCard}
    val anyWildCard = "*" ^^^ { AnyWildCard}

    val term = raw"""[-a-zA-Z0-9'.]+""".r ^^ {Term(_)}

    val partOfSpeech =
      ("adj." | "n." | "v." | "prep." | "det." |  "adv.") ^^ { POS(_)}

    val simWords =
      "~" ~ raw"""[-a-zA-Z'.]+""".r ~
        opt("%" ~> decimalNumber) ~
        opt("#" ~> """[0-9]+""".r) ^^ {
          case  "~" ~ word ~ bound ~ topN  =>
            println("bound: " +bound.getOrElse("none") + "topN: " + topN.getOrElse("none"))
            SimWords(word,bound.map(_.toDouble).getOrElse(0.1), topN.map(_.toInt).getOrElse(10) )
        }

    val nonWildCard = simWords | partOfSpeech | term 
    val or = (nonWildCard <~ "/") ~
      rep1sep(nonWildCard, raw"/")  ^^ { case t~ts => Or(t :: ts) }
    val hereAtom = wildCard | or | nonWildCard
    val maybe = "?" ~> hereAtom ^^ { Maybe(_) }



    val atom: Parser[Atom] = anyWildCard | maybe | hereAtom
    val expr: Parser[List[Atom]] = rep(atom) 
    def parse(userQuery : String) = parseAll(expr, userQuery)
  }

  def handleHereAtom(queries: List[LinggleQuery], hereAtom: HereAtom): List[LinggleQuery] =
    hereAtom match {
      case Or(nonWCs) =>
        for {
          nonWC <- nonWCs
          newLQ <- handleHereAtom(queries, nonWC)
        } yield newLQ

      case SimWords(term, bound, topN) =>
        val simwords = PantelSimWords.get(term, bound, topN) map { x =>
          x.simWords
        } getOrElse Map[String, Double]()
        for {
          (word, score) <- (term, 1.0) :: simwords.toList
          if score > 0.1
          newLQ <- handleHereAtom(queries, Term(word))
        } yield newLQ

      case _ => 
        for {
          LinggleQuery(ts, l, ps, fs) <- queries
          newL = l + 1
          if newL <= 5
        } yield hereAtom match {
          case Term(newT) => LinggleQuery(ts :+ newT, newL, ps :+ l, fs)
          case WildCard => LinggleQuery(ts, newL, ps, fs)
          case POS(pos) => LinggleQuery(ts, newL, ps, fs :+ (l, pos))
        }
    }
  

  def handleAtom(queries: List[LinggleQuery], atom: Atom): List[LinggleQuery] =
    atom match {
      case ha:HereAtom => handleHereAtom(queries, ha)

      case Maybe(hereAtom) => for {
        newLQs <- List(queries, handleHereAtom(queries, hereAtom))
        newLQ <- newLQs
      } yield newLQ

      case AnyWildCard => for {
        LinggleQuery(ts, l, ps, fs) <- queries
        newL <- l to 5
      } yield LinggleQuery(ts, newL, ps, fs)
    }

  def parse(userQuery: String) = 
    QueryParser.parse(userQuery) map { atoms =>
      (atoms.foldLeft
        (List[LinggleQuery](LinggleQuery(Vector(), 0, Vector(), Vector())))
        (handleAtom(_,_)) 
        filter {case LinggleQuery(ts, l, ps, fs) => ts.size > 0})
    }

  def queryDemo(query: String) = {
    import scala.io.AnsiColor._
    println(QueryParser.parse(query).get)
      println(
        s"""query:$query
         |%s
         |""".format( parse(query).get.mkString("  ", "\n  ", "")).stripMargin)
    }

  // def main(args: Array[String]) {
  //   queryDemo("a b c")
  // }
}


case class Row(ngram: Vector[String] = Vector(), count: Long = 0, positions: Vector[Int] = Vector())

class Linggle(hBaseConfFileName: String, table: String) {

  implicit object CountOrdering extends scala.math.Ordering[Row] {
    def compare(a: Row, b: Row) = - (a.count compare b.count)
  }

  val bncJsonPath = "bncwordlemma.json"

  val posMapOpt = current.resourceAsStream(bncJsonPath).map { is =>  POS(is) } orElse {
  // Didn't find the resource, are we in dev / test / stage environment?
    Logger.warn("Could not find %s as a jar resource, will look for a conf directory".format(bncJsonPath))
    Play.getExistingFile("conf/%s".format(bncJsonPath)).map { file => POS(file) }
  }
  val posMap = posMapOpt.get
  val hBaseConfFileIS = current.resourceAsStream("hbase-site.xml").get

  val hTable = hTableConnect(hBaseConfFileIS, table)

  def hTableConnect(hBaseConfFileIS: InputStream, table: String):HTable = {
    val conf = new Configuration
    conf.addResource(hBaseConfFileIS)
    val config = HBaseConfiguration.create(conf)
    new HTable(config, table)
  }


  def timeit[T](run :  => T): T = {
    def t = System.currentTimeMillis
    val t1 = t
    val ret = run
    // println(t - t1)
    ret
  }

  // def timeitGet(linggleQuery: LinggleQuery): (Long,Stream[Row]) = {
  //   timeit(() => get(linggleQuery))
  // }



  def rowFilter(lq: LinggleQuery)(row :Row) : Boolean = {
    val posTagTrans = Map( "n." -> "n", "v." -> "v", "det." -> "d", "prep." -> "p", "adj." -> "a", "adv." -> "r")
    lq.filters forall { case (position, posTag) => posMap(row.ngram(position), posTagTrans(posTag)) }
  }

  def hbaseGet(linggleQuery: LinggleQuery): (Long, Stream[Row]) = {
    println(s"get: $linggleQuery")
    
    val LinggleQuery(terms, length, positions, filters) = linggleQuery
    val row = s"""${length}-${positions.mkString} ${terms.mkString(" ")}""".getBytes
    val getter = new Get(row)
    val result = hTable.get(getter).getValue("data".getBytes, "".getBytes)
    if (result != null) {
      val data = Bytes.toString(result).split("\n")
      val totalCount = data(0).toLong
      val ngramCounts = data drop 1 map { x =>
        val Array(_ngram, _count) = x.split("\t", 2)
        val ngram = _ngram.split(" ").toVector
        val count = _count.toLong
        Row(ngram, count, positions)
      }
      (totalCount, ngramCounts.toStream.filter(rowFilter(linggleQuery)(_)))
    }
    else (0, Stream())
  }
    


  //   def resultToRow(result: Result): Row = {
  //     val value = new String(result.getValue("sel".getBytes, columnBytes))
  //     val Array(_ngram, _count) = value.split("\t")
  //     val count = _count.toInt
  //     val ngram = _ngram.split(" ").toVector
  //     Row(ngram, count, positions)
  //   }
      

  def merge(ss: Seq[Stream[Row]]): Stream[Row] = {
    if (ss.size  == 0 )
      Stream.Empty
    else if (ss.size == 1) ss.head
    else {
      val (max, others) = (ss drop 1).foldLeft(
        (ss.head, List[Stream[Row]]())
      ){
        case ((Stream.Empty, oss), s) =>
          (s, oss)
        case ((m, oss), Stream.Empty) =>
          (m, oss)
        case ((m, oss), s) =>
          if (s.head.count > m.head.count )
            (s, m :: oss )
          else
            (m, s :: oss)
      }
      max match {
        case Stream.Empty =>
          Stream.Empty
        case _ =>
          max.head #:: merge( max.tail :: others)

          }
    }
  }

  def get(q: String): (Long, Stream[Row]) =
  {
    val lqs: List[LinggleQuery] = LinggleQuery.parse(q) getOrElse Nil
    Logger.info(s"$q -> $lqs")

    lqs match {
      case Nil =>
        (0, Stream.Empty)
      case _ =>
        val  (totalCounts, manyRows) = (
          for {
          lq <- lqs.par
          } yield timeit(hbaseGet(lq))
        ).seq.unzip

        (totalCounts.sum, merge(manyRows))
    }
  }
}


object Tester {
  def linggle = {
    println(LinggleQuery.parse("kill the * ").get.head)
    val lgl =new Linggle("hbase-site.xml", "web1t-linggle")
    val (totalCount, rows) =lgl get "kill the *"
    (rows take 100).toList
  }
}




