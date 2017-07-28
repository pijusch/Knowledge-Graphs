package net.sansa_stack.examples.spark.rdf

import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import org.apache.hadoop.io.compress.BZip2Codec
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

import org.apache.hadoop.conf.Configuration
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.openide.NotifyDescriptor.Exception

import scala.Exception




object New {

  def main(args: Array[String]) = {

    val sparkSession=SparkSession.builder
      .master("local[20]")
      .config("spark.network.timeout",10000000)
      .config("spark.local.dir","/data/piyush/spark-temp")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple to ID Mapper ()")
      .getOrCreate()

    sparkSession.sqlContext.sql("set spark.sql.shuffle.partitions=2001")
    println(sparkSession.sparkContext.getConf.get("spark.network.timeout"))


    val langID = List ("de","ja","nl","es")

    var triplesRDD = sparkSession.sparkContext.emptyRDD[String]

    for(lang<-langID) {

      var path = "/data/piyush/Datasets_copy/".concat(lang).concat("/URIs.bz2")

      triplesRDD = triplesRDD.union(sparkSession.sparkContext.textFile(path))

    }

    for(x <- 0 to 3){

      var path = "/data/piyush/Datasets_copy/fr/URIs_".concat(x.toString).concat(".bz2")

      triplesRDD = triplesRDD.union(sparkSession.sparkContext.textFile(path))

    }

    for(x <- 0 to 19){

      var path = "/data/piyush/Datasets_copy/en/URIs_".concat(x.toString).concat(".bz2")

      triplesRDD = triplesRDD.union(sparkSession.sparkContext.textFile(path))

    }

      triplesRDD = triplesRDD.filter(_.toString().indexOf("http://dbpedia.org/property/")<0)


    triplesRDD.saveAsTextFile("/data/piyush/filtered/".concat("URI"),classOf[BZip2Codec])

    /*var normal = triplesRDD.map(x => (x.getSubject, x.getPredicate, x.getObject))


    var grph: TripleRDD = triplesRDD


    var entities = grph.getSubjects.union(grph.getObjects).distinct()

    var relations  = grph.getPredicates.distinct()

    var entity_rdd = entities.zipWithIndex()

    var relations_rdd = relations.zipWithIndex()


    entity_rdd.map(x => x._1+"\t"+x._2).saveAsTextFile("/data/filtered/".concat("Entity2id"),classOf[BZip2Codec])
    relations_rdd.map(x => x._1+"\t"+x._2).saveAsTextFile("/data/filtered/".concat("Rel2id"),classOf[BZip2Codec])*/

    sparkSession.stop
  }

}

