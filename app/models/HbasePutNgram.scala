package cc.nlplab

// import com.twitter.scalding._

import java.io.FileInputStream
import java.nio.file.{Paths, Files}


import org.apache.hadoop.conf.Configuration
// import scala.util.parsing.json.JSON

import scala.collection.JavaConversions._

import io.Source
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.{Bytes, Writables}




// UnigramMap storing "term":String -> idx:Int and idx:Int -> "term":String

object HbasePutNgram {
  val md = java.security.MessageDigest.getInstance("SHA-1")

  def joinString(s: String, ss: String* ) = ss mkString s

  def ngramFilter(unigramMap: UnigramMap)(ngram: Vector[String] ): Boolean = {
    val WORDS_RE = raw"""'?[a-zA-Z]+(['.][a-zA-Z]+)*\.?$$"""
    val END_SYMBOL_RE = raw"[-;,:.?!]$$"
    val INIT_SYMBOL_RE = raw"[-;,:]$$"
    val SENTENCE_TAG_RE = raw"</?S>$$"
    val LAST_RE = joinString("|", WORDS_RE, END_SYMBOL_RE, SENTENCE_TAG_RE)
    val INIT_RE = joinString("|", WORDS_RE, INIT_SYMBOL_RE, SENTENCE_TAG_RE)

    def initMatch(unigramMap: UnigramMap)(word: String ) =
      word.matches(INIT_RE) && (unigramMap contains word)

    def lastMatch(unigramMap: UnigramMap)(word: String ) =
      word.matches(LAST_RE) && (unigramMap contains word)

    (ngram.init forall initMatch(unigramMap)) && lastMatch(unigramMap)(ngram.last)
  }

  def selectNgram(ngram: Vector[String], selector: Set[Int]) = {
    ngram.zipWithIndex filter( selector contains _._2 ) map (_._1)
  }

  def ngramHasher(ngram: Vector[String]) = md.digest(ngram.mkString(" ").getBytes).slice(0,4)

  def toRowKey(ngram: Vector[String], count:Int, selector: Set[Int], unigramMap: UnigramMap): Array[Byte] = {
    val selectedNgram = selectNgram(ngram, selector)
    val selectedBytes = ( selectedNgram map { ss =>
      Bytes.toBytes(unigramMap(ss))
    } ) reduce {_++_}
    val countBytes =  Bytes.toBytes(Int.MaxValue - count)
    val ngramHashBytes = ngramHasher(ngram)
    selectedBytes ++ Array[Byte](0) ++ countBytes ++ ngramHashBytes
  }

  def toNgramPuts(ngram: Vector[String], count: Int, unigramMap: UnigramMap): Iterator[Put] =  {

    val selectors = (List.range(0, ngram.length).toSet.subsets drop 1)
    selectors map { sel =>
      val rowKey = toRowKey(ngram, count, sel, unigramMap)
      val column = s"${ngram.length}-${sel.mkString}".getBytes
      val value = (ngram.mkString(" ") + "\t" + count.toString).getBytes
      val put_data = new Put(rowKey)
      put_data.add("sel".getBytes, column, value)
    }
  }

  def hbaseInit(hbaseTblName: String, conf: Configuration):HTable = {
    val config = HBaseConfiguration.create(conf)
    val hbase = new HBaseAdmin(config)

    if (!hbase.tableExists(hbaseTblName)) {
      println(s"\033[1;33mhbase table doesn't exist, creating...: $hbaseTblName\033[m")
      val mathTable = new HTableDescriptor(hbaseTblName)
      val gradeCol = new HColumnDescriptor("sel")
      mathTable.addFamily(gradeCol)
      hbase.createTable(mathTable)
    } else println(s"\033[1;33m$hbaseTblName exists\033[m")

    new HTable(config, hbaseTblName)
  }

  def main(args: Array[String]) {

    // val confFileNames = Vector("core-site.xml",  "hbase-policy.xml",  "hbase-site.xml",  "hdfs-site.xml",  "mapred-site.xml",  "yarn-site.xml")
    val confFileNames = Vector("hbase-site.xml")
    val conf = new Configuration
    confFileNames foreach { path => 
      conf.addResource(new FileInputStream(path))}

    // val currentDir =  new java.io.File(".")
    // println(currentDir.listFiles.mkString("Files: ", ", ", ""))


    // confFileNames foreach { path => 
    //   if (Files.exists(Paths.get(path))) 
    //     println(s"file exists: $path") 
    //   else println(s"file not exists: $path")
    // }


    // println("\033[1;33m get(hbase.zookeeper.quorum) " + conf.get("hbase.zookeeper.quorum")+ "\033[m")

    val unigramMap = UnigramMap(args(0))
    val hbaseTable = hbaseInit(args(1), conf)

    val NGRAM_RE = raw"""([^\t]+)\t([0-9]+)""".r
    val ngramPuts = for {                                  // ngramPuts: iterator
      NGRAM_RE(_ngram, _count) <- io.Source.stdin.getLines // getLines: iterator
      ngram = _ngram.split(" ").toVector
      if ngramFilter(unigramMap)(ngram)
      count = _count.toInt
      put <- toNgramPuts(ngram, count, unigramMap) 
    } yield put 

    // var lastMs = System.currentTimeMillis
    // var lastIdx = 0
    val groupSize = 5000
    (ngramPuts grouped groupSize).zipWithIndex foreach { case (puts, idx) =>
      // if ((System.currentTimeMillis - lastMs) > 5000) {
      //   println((idx - lastIdx) * groupSize)
      //   lastIdx = idx
      //   lastMs = System.currentTimeMillis
      // }
      hbaseTable.put(puts)
      // println(puts.mkString("-------\n","\n", ""))
      // println(10000)
    }
  }
}

// object Tester {
//   import HbasePutNgram._
//   val unigramMap = UnigramMap("web1t_unigrams_300000up.json")
//   println(toRowKey(Vector("hello", "world", "I", "am", "Joe"), 123, Set(0,2), unigramMap))
// }

