package cc.nlplab

import play.api.Play.current
import play.api.{Logger,Play}
// import    play.Logger
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get,Scan,ResultScanner,Result}
import org.apache.hadoop.conf.Configuration
import java.io.FileInputStream
import java.io.InputStream


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

  case class Maybe(expr: HereAtom) extends Atom
  case object AnyWildCard extends Atom


  import scala.util.parsing.combinator._

  object QueryParser extends JavaTokenParsers {
    val wildCard  = "_".r ^^^ { WildCard}
    val anyWildCard = "*" ^^^ { AnyWildCard}

    val term = raw"""[a-zA-Z0-9'.]+""".r ^^ {Term(_)}
    val partOfSpeech = ("adj." | "n." | "v." | "prep." | "det." |  "adv.") ^^ { POS(_)}

    val nonWildCard = partOfSpeech | term
    val or = (nonWildCard <~ "|") ~ rep1sep(nonWildCard, raw"|")  ^^ { case t~ts => Or(t :: ts) }
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

  def main(args: Array[String]) {
    queryDemo("a b c")
  }
}


case class Row(ngram: Vector[String] = Vector(), count: Long = 0, positions: Vector[Int] = Vector())

class Linggle(hBaseConfFileName: String, table: String) {

  implicit object CountOrdering extends scala.math.Ordering[Row] {
    def compare(a: Row, b: Row) = - (a.count compare b.count)
  }

  // Play.getExistingFile("")
  val bncJsonPath = "bncwordlemma.json"
  // val bncJsonIS = current.resourceAsStream(bncJsonPath)

  val posMapOpt = current.resourceAsStream(bncJsonPath).map { is =>  POS(is) } orElse {
  // Didn't find the resource, are we in dev / test / stage environment?
    Logger.warn("Could not find %s as a jar resource, will look for a conf directory".format(bncJsonPath))
    Play.getExistingFile("conf/%s".format(bncJsonPath)).map { file => POS(file) }
  }
  val posMap = posMapOpt.get
  // val posMap = (src.map { is =>
    // POS(src)
  // }).get
  // orElse {
      // Logger.warn("Could not find %s as a jar resource, will look for a conf directory".format(bncJsonPath))
  // }

  // val unigramMap = UnigramMap(unigramMapJson)
  val hBaseConfFileIS = current.resourceAsStream("hbase-site.xml").get

  val hTable = hTableConnect(hBaseConfFileIS, table)

  def hTableConnect(hBaseConfFileIS: InputStream, table: String):HTable = {
    val conf = new Configuration
    conf.addResource(hBaseConfFileIS)
    val config = HBaseConfiguration.create(conf)
    new HTable(config, table)
  }

  // def toHex(buf: Array[Byte]): String =
  //   buf.map("\\%02X" format _).mkString


  // def buildScanner(terms: Vector[String], columnBytes: Array[Byte] ): ResultScanner = {
  //   val startRow = (terms
  //     map (t => Bytes.toBytes(unigramMap(t)))) reduce {_++_}
  //   val stopRow: Array[Byte] = startRow.init :+ (startRow.last + 1).toByte
  //   val scan = new Scan(startRow, stopRow)
  //   // val scan = new Scan(startRow)
  //   scan.setSmall(true)
  //   scan.setBatch(100)
  //   scan.setCacheBlocks(false)
  //   scan.setMaxVersions(1)
  //   // scan.setMaxResultSize(100)
  //   scan.setCaching(500)
  //   scan.setCacheBlocks(true)
  //   scan.setFilter(new PageFilter(100));
  //   // scan.setAttribute(Scan.HINT_LOOKAHEAD, Bytes.toBytes(5));

  //   // scan.setMaxResultsPerColumnFamily(100)


  //   scan.addColumn("sel".getBytes, columnBytes)
  //   hTable.getScanner(scan)
  // }

  def timeit[T](run : () => T): T = {
    def t = System.currentTimeMillis
    val t1 = t
    val ret = run()
    println(t - t1)
    ret
  }

  def timeitGet(linggleQuery: LinggleQuery): Stream[Row] = {
    timeit(() => get(linggleQuery))
  }



  def rowFilter(lq: LinggleQuery)(row :Row) : Boolean = {
    val posTagTrans = Map( "n." -> "n", "v." -> "v", "det." -> "d", "prep." -> "p", "adj." -> "a", "adv." -> "r")
    lq.filters forall { case (position, posTag) => posMap(row.ngram(position), posTagTrans(posTag)) }
  }

  def get(linggleQuery: LinggleQuery): Stream[Row] = {
    println(s"get: $linggleQuery")
    
    val LinggleQuery(terms, length, positions, filters) = linggleQuery
    val row = s"""${length}-${positions.mkString} ${terms.mkString(" ")}""".getBytes
    val hbaseGet = new Get(row)
    val result = hTable.get(hbaseGet).getValue("data".getBytes, "".getBytes)
    if (result != null) {
      val data = Bytes.toString(result).split("\n")
      val totalCount = data(0).toLong
      val ngramCounts = data drop 1 map { x =>
        val Array(_ngram, _count) = x.split("\t", 2)
        val ngram = _ngram.split(" ").toVector
        val count = _count.toLong
        Row(ngram, count, positions)
      }
      ngramCounts.toStream.filter(rowFilter(linggleQuery)(_))
    }
    else Stream()
  }

  // def scan(linggleQuery: LinggleQuery): Stream[Row] = {
  //   println(s"scan: $linggleQuery")
    
  //   val LinggleQuery(terms, length, positions, filters) = linggleQuery
  //   val column = s"${length}-${positions.mkString}"
  //   val columnBytes = column.getBytes

  //   val scanner =  buildScanner(terms, columnBytes)


  //   def resultToRow(result: Result): Row = {
  //     val value = new String(result.getValue("sel".getBytes, columnBytes))
  //     val Array(_ngram, _count) = value.split("\t")
  //     val count = _count.toInt
  //     val ngram = _ngram.split(" ").toVector
  //     Row(ngram, count, positions)
  //   }
      
  //   // def scanToStream(scanner: ResultScanner): Stream[Row] = {
  //   //   val rows = (scanner.next(100) map resultToRow)
  //   //   if (rows.size < 100)
  //   //   {
  //   //     try
  //   //       rows.toStream
  //   //     finally
  //   //       scanner.close()
  //   //   }
  //   //   else
  //   //     rows.toStream #::: scanToStream(scanner)
  //   // }

  //   // scanToStream(scanner) 
  //   val s = (scanner.next(100) map resultToRow ).toStream
  //   scanner.close()
  //   s
  // }

  // def merge(ss: List[Stream[Row]] ): Stream[Row] = {
  //   val head #:: tail = s0
  //   head #:: merge(List(tail, s1, s2, s3, s4))
  //   unfold(ss)(f) //f: List[Stream[Row]] -> (Row, List[Stream[Row]])
  //   val ones : Stream[Int] = 1 #:: ones
  //   val nats = 0 #:: nats zipWith (+) ones
  // }

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


  // def rowFilter(lq: LinggleQuery)(row :Row) : Boolean = {
  //   val posTagTrans = Map( "n." -> "n", "v." -> "v", "det." -> "d", "prep." -> "p", "adj." -> "a", "adv." -> "r")
  //   lq.filters forall { case (position, posTag) => posMap(row.ngram(position), posTagTrans(posTag)) }
  // }

  

  def query(q: String): Stream[Row] = {

    Logger.info("query: $q")
    val lqs: List[LinggleQuery] = LinggleQuery.parse(q).get
    val results = for {
      lq <- lqs.par
      // row = timeitGet(lq) take 100
      // if rowFilter(lq)(row)
    } yield timeitGet(lq) take 1000
    val rows = merge(results.seq)
    // rows.seq.sorted.toList     // TODO: merge sort
    rows
  }
}


object Tester {
  def linggle = {
    println(LinggleQuery.parse("kill the * ").get.head)

    val lgl =new Linggle("hbase-site.xml", "web1t-linggle")

    (lgl.query("kill the *") take 100).toList
  }
}




