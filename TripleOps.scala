package net.sansa_stack.examples.spark.rdf


import java.net.{URI => JavaURI}

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.jena.graph.{Node, Node_URI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object TripleOps {

  def main(args: Array[String]) = {

    val input = "/home/mypc/Desktop/daad/sample.ttl"
    val optionsList = args.drop(1).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    options.foreach {
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    println("======================================")
    println("|        Triple Ops example       |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple Ops example (" + input + ")")
      .getOrCreate()

    val ops = JenaSparkRDDOps(sparkSession.sparkContext)
    import ops._

    val triplesRDD = NTripleReader.load(sparkSession, JavaURI.create(input))


    val graph: TripleRDD = triplesRDD

//    //Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
//    println("All triples related to Dickens:\n" + graph.find(URI("http://dbpedia.org/resource/Charles_Dickens"), ANY, ANY).collect().mkString("\n"))
//
//    //Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
//    println("All triples for predicate influenced:\n" + graph.find(ANY, URI("http://dbpedia.org/ontology/influenced"), ANY).collect().mkString("\n"))
//
//    //Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
//    println("All triples influenced by Henry_James:\n" + graph.find(ANY, ANY, URI("<http://dbpedia.org/resource/Henry_James>")).collect().mkString("\n"))
//
//    println("Number of triples: " + graph.find(ANY, ANY, ANY).distinct.count())
//    println("Number of subjects: " + graph.getSubjects.distinct.count())
//    println("Number of predicates: " + graph.getPredicates.distinct.count())
//    println("Number of objects: " + graph.getPredicates.distinct.count())
//
//    val subjects = graph.filterSubjects(_.isURI()).collect.mkString("\n")
//
//    val predicates = graph.filterPredicates(_.isVariable()).collect.mkString("\n")
//    val objects = graph.filterObjects(_.isLiteral()).collect.mkString("\n")

    //graph.getTriples.take(5).foreach(println(_))

    var blank_s  = graph.filterSubjects(_.isBlank())
    var blank_o = graph.filterObjects(_.isBlank())

    var blank = blank_o.union(blank_s).distinct()



    var temp: TripleRDD = graph.filterObjects(_.isLiteral())

    var literals = temp.filterSubjects(_.isURI())

    temp = graph.filterObjects(_.isURI())

    var normal: RDD[(Node, Node, Node)] = temp.filterSubjects(_.isURI()).map(x => (x.getSubject, x.getPredicate, x.getObject))

    var subjects = graph.getSubjects.filter(_.isURI)

    var objects = graph.getObjects.filter(_.isURI)

    var entities = subjects.union(objects).distinct()




    var entity_rdd = subjects.zipWithUniqueId()

    var pred_rdd: RDD[(Node, Long)] = graph.getPredicates.filter(_.isURI).zipWithUniqueId()

    val subjectKeyTriples = normal.map(x => (x._1, (x._2, x._3)))


    val joinedBySubject: RDD[(Node, (Long, (Node, Node)))] = entity_rdd.join(subjectKeyTriples)

    val subjectMapped: RDD[(Long, Node, Node)] = joinedBySubject.map{
      case (oldSubject: Node, newTriple: (Long, (Node, Node))) =>
        (newTriple._1, newTriple._2._1, newTriple._2._2)
    }

    val objectKeyTriples: RDD[(Node, (Long, Node))] = subjectMapped.map(x => (x._3, (x._1,x._2)))

    val joinedByObject: RDD[(Node, (Long, (Long, Node)))] = entity_rdd.join(objectKeyTriples)

    val objectMapped: RDD[(Long, Node, Long)] = joinedByObject.map{
      case (oldObject: Node, newTriple: (Long, (Long, Node))) =>
        (newTriple._2._1,newTriple._2._2,newTriple._1)
    }


    val predKeyTriples: RDD[(Node, (Long, Long))] = objectMapped.map(x => (x._2,(x._1,x._3)))

    val joinedByPred = pred_rdd.join(predKeyTriples)

    val predMapped = joinedByPred.map{
      case (oldPred: Node_URI, newTriple: (Long, (Long, Long))) =>
        (newTriple._2._1, newTriple._1, newTriple._2._2 )
    }

    predMapped.foreach(println)




    //graph.filterObjects(_.isBlank())

    sparkSession.stop

  }

}