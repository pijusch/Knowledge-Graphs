package net.sansa_stack.examples.spark.rdf

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import java.io._
import java.lang.RuntimeException

import scala.io.Codec
import java.nio.charset.Charset
import java.net.{HttpURLConnection, MalformedURLException, URL, URI => JavaURI}

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.jena.graph.{Node, Node_URI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions
import scala.collection.mutable
import scala.language.implicitConversions
import java.io.{BufferedReader, Reader}
import java.io.{File, InputStream, OutputStream}
import java.nio.file.{Files, Paths}

import org.apache.jena.graph
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.util.Try
import sys.process._
import java.io.File
import java.lang.Exception

import org.openide.NotifyDescriptor.Exception

import scala.Exception




/**
  * Allows common handling of java.io.File and java.nio.file.Path
  */
abstract class FileLike[T] {

  /**
    * @return full path
    */
  def toString: String

  /**
    * @return file name, or null if file path has no parts
    */
  def name: String

  def resolve(name: String): Try[T]

  def names: List[String]

  def list: List[T]

  def exists: Boolean

  @throws[java.io.IOException]("if file does not exist or cannot be deleted")
  def delete(recursive: Boolean = false): Unit

  def size(): Long

  def isFile: Boolean

  def isDirectory: Boolean

  def hasFiles: Boolean

  def inputStream(): InputStream

  def outputStream(append: Boolean = false): OutputStream

  def getFile: File
}


object RichReader
{
  implicit def wrapReader(reader: BufferedReader) = new RichReader(reader)

  implicit def wrapReader(reader: Reader) = new RichReader(reader)
}


class RichReader(reader: BufferedReader) {

  def this(reader: Reader) = this(new BufferedReader(reader))

  /**
    * Process all lines. The last value passed to proc will be null.
    */
  def foreach[U](proc: String => U): Unit = {
    while (true) {
      val line = reader.readLine()
      proc(line)
      if (line.isEmpty()) return
    }
  }
}

object IOUtils {

  /**
    * Map from file suffix (without "." dot) to output stream wrapper
    */
  val zippers = Map[String, OutputStream => OutputStream] (
    "gz" -> { new GZIPOutputStream(_) },
    "bz2" -> { new BZip2CompressorOutputStream(_) }
  )

  /**
    * Map from file suffix (without "." dot) to input stream wrapper
    */
  val unzippers = Map[String, InputStream => InputStream] (
    "gz" -> { new GZIPInputStream(_) },
    "bz2" -> { new BZip2CompressorInputStream(_, true) }
  )

  /**
    * use opener on file, wrap in un/zipper stream if necessary
    */
  private def open[T](file: FileLike[_], opener: FileLike[_] => T, wrappers: Map[String, T => T]): T = {
    val name: String = file.name
    val suffix = name.substring(name.lastIndexOf('.') + 1)
    wrappers.getOrElse(suffix, identity[T] _)(opener(file))
  }

  /**
    * open output stream, wrap in zipper stream if file suffix indicates compressed file.
    */
  def outputStream(file: FileLike[_], append: Boolean = false): OutputStream =
    open(file, _.outputStream(append), zippers)

  /**
    * open input stream, wrap in unzipper stream if file suffix indicates compressed file.
    */
  def inputStream(file: FileLike[_]): InputStream =
    open(file, _.inputStream(), unzippers)

  /**
    * open output stream, wrap in zipper stream if file suffix indicates compressed file,
    * wrap in writer.
    */
  def writer(file: FileLike[_], append: Boolean = false, charset: Charset = Codec.UTF8.charSet): Writer =
    new OutputStreamWriter(outputStream(file, append), charset)

  /**
    * open input stream, wrap in unzipper stream if file suffix indicates compressed file,
    * wrap in reader.
    */
  def reader(file: FileLike[_], charset: Charset = Codec.UTF8.charSet): Reader =
    new InputStreamReader(inputStream(file), charset)

  /**
    * open input stream, wrap in unzipper stream if file suffix indicates compressed file,
    * wrap in reader, wrap in buffered reader, process all lines. The last value passed to
    * proc will be null.
    */
  def readLines(file: FileLike[_], charset: Charset = Codec.UTF8.charSet)(proc: String => Unit): Unit = {
    val reader: Reader = this.reader(file)

    import RichReader._
    try {

      for (line <- reader) {
        proc(line)
      }
    }
    finally reader.close()
  }

  /**
    * Copy all bytes from input to output. Don't close any stream.
    */
  def copy(in: InputStream, out: OutputStream) : Unit = {
    val buf = new Array[Byte](1 << 20) // 1 MB
    while (true)
    {
      val read = in.read(buf)
      if (read == -1)
      {
        out.flush
        return
      }
      out.write(buf, 0, read)
    }
  }

}

object TripleOps {

  def main(args: Array[String]) = {

    val sparkSession=SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple to ID Mapper ()")
      .getOrCreate()


    val langID= List("en","de","es","fr","ja","nl")
    val whiteList = List("mappingbased_objects_disjoint_domain","topical_concepts")
    //val whiteList=List("anchor_text","article_categories","article_templates","category_labels","citation_data","citation_links","cited_facts","disambiguations","external_links","freebase_links","french_population","genders","geo_coordinates","geo_coordinates_mappingbased","geonames_links","homepages","images","infobox_properties","infobox_properties_mapped","infobox_properties_definitions","infobox_test","instance_types","instance_types_dbtax_dbo"
    //    ,"instance_types_dbtax_ext","instance_types_lhd_dbo","instance_types_lhd_ext","instance_types_sdtyped_dbo","instance_types_transitive","interlanguage_links","interlanguage_links_chapters","labels","long_abstracts","mappingbased_literals")


    for(lang<-langID){
      var dataset: RDD[graph.Triple] =sparkSession.sparkContext.emptyRDD[graph.Triple]
      val savePath="/home/mypc/Downloads/datasets/".concat(lang).concat("/")
      for(filename<-whiteList) {
        val link = "http://downloads.dbpedia.org/2016-04/core-i18n/"
        val link1 = link.concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
        //1) Check if link is valid URL.
        //2) If it is,download it and do the above work and merge to the rdd.
        //3) If it isnt,use try catch or something.
        val connection = (new URL(link1)).openConnection.asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("HEAD")
        connection.connect()
        if (connection.getResponseCode() != 404) { //Can also have !=404
          //          println("URL Exists")
          //Insert code here to download the file.
          new URL(link1) #> new File(savePath.concat(filename).concat("_").concat(lang).concat(".ttl.bz2")) !!
          val path = "/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
          val fin = Files.newInputStream(Paths.get(path))
          val in = new BufferedInputStream(fin)
          val bzIn = new BZip2CompressorInputStream(in)
          val reader = new BufferedReader(new InputStreamReader(bzIn, Codec.UTF8.charSet))
          val a = Iterator.continually(reader.readLine()).takeWhile { r =>
            r != null
          }.toSeq
          a.init.filterNot(_ == null) -> (a.last != null)
          var rdd = sparkSession.sparkContext.parallelize(a)
          rdd = rdd.filter(x => (x.charAt(0)=='<'))
          val triplesRDD = rdd.map(line =>
            RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())

          dataset = dataset.union(triplesRDD)
        }

      }

          val grph: TripleRDD = dataset

          var blank_s  = grph.filterSubjects(_.isBlank())
          var blank_o = grph.filterObjects(_.isBlank())

          var blank = blank_o.union(blank_s).distinct()



          var temp: TripleRDD = grph.filterObjects(_.isLiteral())

          var literals = temp.filterSubjects(_.isURI())

          temp = grph.filterObjects(_.isURI())

          var normal: RDD[(Node, Node, Node)] = temp.filterSubjects(_.isURI()).map(x => (x.getSubject, x.getPredicate, x.getObject))

          var subjects = grph.getSubjects.filter(_.isURI)

          var objects = grph.getObjects.filter(_.isURI)

          var entities = subjects.union(objects).distinct()




          var entity_rdd = subjects.zipWithUniqueId()

          var pred_rdd: RDD[(Node, Long)] = grph.getPredicates.filter(_.isURI).zipWithUniqueId()


          entity_rdd.coalesce(1,true).saveAsTextFile("/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat("Entity2id"))
          pred_rdd.coalesce(1,true).saveAsTextFile("/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat("Rel2id"))

      for(filename<-whiteList) {

          val path = "/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
          val file = new File(path)
          val fin: InputStream = Files.newInputStream(Paths.get(path))
        if(file.isFile()) {
          val in = new BufferedInputStream(fin)
          val bzIn = new BZip2CompressorInputStream(in)
          val reader = new BufferedReader(new InputStreamReader(bzIn, Codec.UTF8.charSet))
          val a = Iterator.continually(reader.readLine()).takeWhile { r =>
            r != null
          }.toSeq
          a.init.filterNot(_ == null) -> (a.last != null)
          var rdd = sparkSession.sparkContext.parallelize(a)
          rdd = rdd.filter(x => (x.charAt(0) == '<'))
          val triplesRDD = rdd.map(line =>
            RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())

          val grph: TripleRDD = triplesRDD

          var blank_s = grph.filterSubjects(_.isBlank())
          var blank_o = grph.filterObjects(_.isBlank())

          var blank = blank_o.union(blank_s).distinct()


          var temp: TripleRDD = grph.filterObjects(_.isLiteral())

          var literals = temp.filterSubjects(_.isURI())

          temp = grph.filterObjects(_.isURI())

          var normal: RDD[(Node, Node, Node)] = temp.filterSubjects(_.isURI()).map(x => (x.getSubject, x.getPredicate, x.getObject))

          var subjects = grph.getSubjects.filter(_.isURI)

          var objects = grph.getObjects.filter(_.isURI)

          var entities = subjects.union(objects).distinct()


          val subjectKeyTriples = normal.map(x => (x._1, (x._2, x._3)))


          val joinedBySubject: RDD[(Node, (Long, (Node, Node)))] = entity_rdd.join(subjectKeyTriples)

          val subjectMapped: RDD[(Long, Node, Node)] = joinedBySubject.map {
            case (oldSubject: Node, newTriple: (Long, (Node, Node))) =>
              (newTriple._1, newTriple._2._1, newTriple._2._2)
          }

          val objectKeyTriples: RDD[(Node, (Long, Node))] = subjectMapped.map(x => (x._3, (x._1, x._2)))

          val joinedByObject: RDD[(Node, (Long, (Long, Node)))] = entity_rdd.join(objectKeyTriples)

          val objectMapped: RDD[(Long, Node, Long)] = joinedByObject.map {
            case (oldObject: Node, newTriple: (Long, (Long, Node))) =>
              (newTriple._2._1, newTriple._2._2, newTriple._1)
          }


          val predKeyTriples: RDD[(Node, (Long, Long))] = objectMapped.map(x => (x._2, (x._1, x._3)))

          val joinedByPred = pred_rdd.join(predKeyTriples)

          val predMapped = joinedByPred.map {
            case (oldPred: Node_URI, newTriple: (Long, (Long, Long))) =>
              (newTriple._2._1, newTriple._1, newTriple._2._2)
          }

          //   entity_rdd.coalesce(1,true).saveAsTextFile("Entity2id.txt")
          //    pred_rdd.coalesce(1,true).saveAsTextFile("Rel2id.txt")
          predMapped.coalesce(1, true).saveAsTextFile("/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat(filename))


        }

        }


      }





//    var file_name  = "sample.bz2"
//
//    try{
//      var url = new URL("http://downloads.dbpedia.org/2015-10/core-i18n/en/pnd_en.ttl.bz2")
//
//      url #> new File(file_name) !!
//    }
//    catch{
//      case e: RuntimeException => println("badURL")
//    }
//
//
//
//    //new URL("http://downloads.dbpedia.org/2015-10/core-i18n/en/topical_concepts_en.ttl.bz2") #> new File("sample.bz2") !!
//
//
//    val fin: InputStream = Files.newInputStream(Paths.get(file_name))
//    val in: BufferedInputStream = new BufferedInputStream(fin)
//    val bzIn: BZip2CompressorInputStream = new BZip2CompressorInputStream(in)
//
//
//    val reader: BufferedReader =new BufferedReader(new InputStreamReader(bzIn,Codec.UTF8.charSet))
//
//    var count = -1
//    val a = Iterator.continually(reader.readLine()).takeWhile { r =>
//      count += 1
//      r!=null
//    }.toSeq
//    a.init.filterNot(_ == null) -> (a.last != null)
//
//
//
//    import RichReader._
//
//
//
//    var rdd: RDD[String] = sparkSession.sparkContext.parallelize(a)
//
//
//
//    var triplesRDD = rdd.map(f = line =>
//      RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
//
//
//
//
//
//
//    //println(temp)
//
////    val temp = sparkSession.sparkContext.parallelize()
//
//
////    val inputStreamBz2: InputStream = IOUtils.inputStream(file = new FileLike[] {
////      override def toString: String = {
////          return "/home/mypc/Desktop/daad/Datasets/persondata_en.ttl.bz2"
////      }
////      override def name: String = {
////        return "persondata_en.ttl.bz2"
////      }
////    })
//
//
//
//
//
//
//
////    val input = "/home/mypc/Desktop/daad/Datasets/*.ttl"
////    val optionsList = args.drop(1).map { arg =>
////      arg.dropWhile(_ == '-').split('=') match {
////        case Array(opt, v) => (opt -> v)
////        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
////      }
////    }
////    val options = mutable.Map(optionsList: _*)
////
////    options.foreach {
////      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
////    }
//    println("======================================")
//    println("|        Triple Ops example       |")
//    println("======================================")
//
////    val sparkSession = SparkSession.builder
////      .master("local[*]")
////      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
////      .appName("Triple Ops example (" + input + ")")
////      .getOrCreate()
////
////    val ops = JenaSparkRDDOps(sparkSession.sparkContext)
////    import ops._
////
////    val triplesRDD = NTripleReader.load(sparkSession, JavaURI.create(input))
////
////
//    val graph: TripleRDD = triplesRDD
//
//
//
////    //Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
////    println("All triples related to Dickens:\n" + graph.find(URI("http://dbpedia.org/resource/Charles_Dickens"), ANY, ANY).collect().mkString("\n"))
////
////    //Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
////    println("All triples for predicate influenced:\n" + graph.find(ANY, URI("http://dbpedia.org/ontology/influenced"), ANY).collect().mkString("\n"))
////
////    //Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
////    println("All triples influenced by Henry_James:\n" + graph.find(ANY, ANY, URI("<http://dbpedia.org/resource/Henry_James>")).collect().mkString("\n"))
////
////    println("Number of triples: " + graph.find(ANY, ANY, ANY).distinct.count())
////    println("Number of subjects: " + graph.getSubjects.distinct.count())
////    println("Number of predicates: " + graph.getPredicates.distinct.count())
////    println("Number of objects: " + graph.getPredicates.distinct.count())
////
////    val subjects = graph.filterSubjects(_.isURI()).collect.mkString("\n")
////
////    val predicates = graph.filterPredicates(_.isVariable()).collect.mkString("\n")
////    val objects = graph.filterObjects(_.isLiteral()).collect.mkString("\n")
//
//    //graph.getTriples.take(5).foreach(println(_))
//
//    var blank_s  = graph.filterSubjects(_.isBlank())
//    var blank_o = graph.filterObjects(_.isBlank())
//
//    var blank = blank_o.union(blank_s).distinct()
//
//
//
//    var temp: TripleRDD = graph.filterObjects(_.isLiteral())
//
//    var literals = temp.filterSubjects(_.isURI())
//
//    temp = graph.filterObjects(_.isURI())
//
//    var normal: RDD[(Node, Node, Node)] = temp.filterSubjects(_.isURI()).map(x => (x.getSubject, x.getPredicate, x.getObject))
//
//    var subjects = graph.getSubjects.filter(_.isURI)
//
//    var objects = graph.getObjects.filter(_.isURI)
//
//    var entities = subjects.union(objects).distinct()
//
//
//
//
//    var entity_rdd = subjects.zipWithUniqueId()
//
//    var pred_rdd: RDD[(Node, Long)] = graph.getPredicates.filter(_.isURI).zipWithUniqueId()
//
//    val subjectKeyTriples = normal.map(x => (x._1, (x._2, x._3)))
//
//
//    val joinedBySubject: RDD[(Node, (Long, (Node, Node)))] = entity_rdd.join(subjectKeyTriples)
//
//    val subjectMapped: RDD[(Long, Node, Node)] = joinedBySubject.map{
//      case (oldSubject: Node, newTriple: (Long, (Node, Node))) =>
//        (newTriple._1, newTriple._2._1, newTriple._2._2)
//    }
//
//    val objectKeyTriples: RDD[(Node, (Long, Node))] = subjectMapped.map(x => (x._3, (x._1,x._2)))
//
//    val joinedByObject: RDD[(Node, (Long, (Long, Node)))] = entity_rdd.join(objectKeyTriples)
//
//    val objectMapped: RDD[(Long, Node, Long)] = joinedByObject.map{
//      case (oldObject: Node, newTriple: (Long, (Long, Node))) =>
//        (newTriple._2._1,newTriple._2._2,newTriple._1)
//    }
//
//
//    val predKeyTriples: RDD[(Node, (Long, Long))] = objectMapped.map(x => (x._2,(x._1,x._3)))
//
//    val joinedByPred = pred_rdd.join(predKeyTriples)
//
//    val predMapped = joinedByPred.map{
//      case (oldPred: Node_URI, newTriple: (Long, (Long, Long))) =>
//        (newTriple._2._1, newTriple._1, newTriple._2._2 )
//    }
//
////   entity_rdd.coalesce(1,true).saveAsTextFile("Entity2id.txt")
////    pred_rdd.coalesce(1,true).saveAsTextFile("Rel2id.txt")
////    predMapped.coalesce(1,true).saveAsTextFile("Triple2id.txt")
//
//
//
//
//
//    //graph.filterObjects(_.isBlank())

    sparkSession.stop

  }

}