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
import org.openide.NotifyDescriptor.Exception

import scala.Exception



object TripleOps {

  def main(args: Array[String]) = {




    val sparkSession=SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple to ID Mapper ()")
      .getOrCreate()

    /*var pathJar =  new Nothing(classOf[Nothing].getProtectionDomain.getCodeSource.getLocation.toURI.getPath)

    sparkSession.sparkContext.addJar(pathJar)*/

    val hadoopConfig: Configuration = sparkSession.sparkContext.hadoopConfiguration

    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)


    val langID= List("en")//"es","fr","ja","nl")
    val whiteList = List("mappingbased_objects_disjoint_domain")//,"infobox_properties_definitions","specific_mappingbased_properties","topical_concepts","homepages")
    //val whiteList=List("anchor_text","article_categories","article_templates","category_labels","citation_data","citation_links","cited_facts","disambiguations","external_links","freebase_links","french_population","genders","geo_coordinates","geo_coordinates_mappingbased","geonames_links","homepages","images","infobox_properties","infobox_properties_mapped","infobox_properties_definitions","infobox_test","instance_types","instance_types_dbtax_dbo"
        //,"instance_types_dbtax_ext","instance_types_lhd_dbo","instance_types_lhd_ext","instance_types_sdtyped_dbo","instance_types_transitive","interlanguage_links","interlanguage_links_chapters","labels","long_abstracts","mappingbased_literals","mappingbased_objects","mappingbased_objects_disjoint_domain",
//"mappingbased_objects_disjoint_range","mappingbased_objects_uncleaned","article_templates_nested","out_degree",
//"page_ids","page_length","page_links","persondata","pnd","redirects","revision_ids","revision_uris","transitive_redirects",
//"short_abstracts","skos_categories","specific_mappingbased_properties","template_parameters","topical_concepts")


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

          println("downloadeding ".concat(filename))
          new URL(link1) #> new File(savePath.concat(filename).concat("_").concat(lang).concat(".ttl.bz2")) !!

          println("downloaded ".concat(filename))
          val path = "/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
          /*val fin = Files.newInputStream(Paths.get(path))
          val in = new BufferedInputStream(fin)
          val bzIn = new BZip2CompressorInputStream(in)
          val reader = new BufferedReader(new InputStreamReader(bzIn, Codec.UTF8.charSet))
          val a = Iterator.continually(reader.readLine()).takeWhile { r =>
            r != null
          }.toSeq
          a.init.filterNot(_ == null) -> (a.last != null)
          var rdd = sparkSession.sparkContext.parallelize(a)*/
          var rdd  = sparkSession.sparkContext.textFile(path)
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




          var entity_rdd = entities.coalesce(1,true).zipWithUniqueId()

          var pred_rdd: RDD[(Node, Long)] = grph.getPredicates.filter(_.isURI).coalesce(1,true).distinct().zipWithUniqueId()


          entity_rdd.coalesce(1,true).map(x => x._1+"\t"+x._2).saveAsTextFile("/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat("Entity2id"))
          pred_rdd.coalesce(1,true).map(x => x._1+"\t"+x._2).saveAsTextFile("/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat("Rel2id"))

      for(filename<-whiteList) {


        val link = "http://downloads.dbpedia.org/2016-04/core-i18n/"
        val link1 = link.concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
        //1) Check if link is valid URL.
        //2) If it is,download it and do the above work and merge to the rdd.
        //3) If it isnt,use try catch or something.
        val connection = (new URL(link1)).openConnection.asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("HEAD")
        connection.connect()
        if (connection.getResponseCode() != 404) {
          val path = "/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")


//          val tm: RDD[String] = sparkSession.sparkContext.textFile("file")
//          val fin: InputStream = Files.newInputStream(Paths.get(path))
//          val in = new BufferedInputStream(fin)
//          val bzIn = new BZip2CompressorInputStream(in)
//          val reader = new BufferedReader(new InputStreamReader(bzIn, Codec.UTF8.charSet))
//          val a = Iterator.continually(reader.readLine()).takeWhile { r =>
//            r != null
//          }.toSeq
//          a.init.filterNot(_ == null) -> (a.last != null)
//          var rdd: RDD[String] = sparkSession.sparkContext.parallelize(a)
          var rdd: RDD[String] = sparkSession.sparkContext.textFile(path)
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

          var normal: RDD[(Node, Node, Node)] = temp.filterSubjects(_.isURI()).map(x => (x.getSubject, x.getPredicate, x.getObject)).coalesce(1,true)

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
          predMapped.coalesce(1, true).map(x => x._1+"\t"+x._2+"\t"+x._3).saveAsTextFile("/home/mypc/Downloads/datasets/".concat(lang).concat("/").concat(filename))//,classOf[BZip2Codec])


        }

        }


      }

    sparkSession.stop

  }

}
