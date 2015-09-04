package org.graphlab.create

import java.io._
import java.net._
import java.nio.charset.Charset
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap, Collections}


import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark._
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

object GraphLabUtil {

  object Mode extends Enumeration {
       type Mode = Value
       val EscapeChar, Normal = Value
  }
  def UnEscapeString(s: String) : String = { 
       val replace_char = '\u001F'
       val output = new StringBuilder()
       import Mode._
       var status = Normal
       for (c: Char <- s) {
         if (c == '\\' && status == Normal) { 
            status = EscapeChar
         }
         else if (status == EscapeChar && c == replace_char) { 
            status = Normal
            output+','
         }
         else if (status == EscapeChar && c == 'n') { 
            status = Normal
            output+'\n'
         }
         else if (status == EscapeChar && c == 'r') { 
            status = Normal
            output+'\r'
         }
         else if (status == EscapeChar && c == 'b') { 
            status = Normal
            output+'\b'
         }
         else if (status == EscapeChar && c == 't') { 
            status = Normal
            output+'\t'
         }
         else if (status == EscapeChar && c == '\"') { 
            status = Normal
            output+'\"'
         }
         else if (status == EscapeChar && c == '\'') { 
            status = Normal
            output+'\''
         }
         else {  
            output+c 
            status = Normal
         }
       } 
       return output.toString
  }
   
  def EscapeString(s: String) : String = { 
    val replace_char = '\u001F'
    val output = new StringBuilder()
    for (c: Char <- s) {
      if (c == '\\') { 
        output+'\\'
        output+c
      }
      else if (c == ',') { 
        output+'\\'
        output+replace_char
      }
      else if (c == '\n') { 
        output+'\\'
        output+'n'
      }
      else if (c == '\b') { 
        output+'\\'
        output+'b'
      }
      else if (c == '\t') { 
        output+'\\'
        output+'t'
      }
      else if (c == '\r') { 
        output+'\\'
        output+'r'
      }
      else if (c == '\'') { 
        output+'\\'
        output+'\''
      }
      else if (c == '\"') { 
        output+'\\'
        output+'\''
      }
      else {  
        output+c 
      }
    } 
    return output.toString
  } 

  def stringToByte(jRDD: JavaRDD[String]): JavaRDD[Array[Byte]] = { 
    jRDD.rdd.mapPartitions { iter =>
      iter.map { row =>
        row match {
            case str:String => new sun.misc.BASE64Decoder().decodeBuffer(str)
          //case str:String => UnEscapeString(str).toCharArray.map(_.toByte)
        } 
      }

    }
  }
 
   
  def byteToString(jRDD: JavaRDD[Array[Byte]]): JavaRDD[String] = { 
    jRDD.rdd.mapPartitions { iter =>
      iter.map { row =>
        row match {
          //case bytes:Array[Byte] => EscapeString(new String(bytes.map(_.toChar)))
           case bytes:Array[Byte] => new sun.misc.BASE64Encoder().encode(bytes).replaceAll("\n","")
        } 
      }

    }
  }
 
  def unEscapeJavaRDD(jRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[Byte]] = {
     //val jRDD = pythonToJava(rdd)
     jRDD.rdd.mapPartitions { iter =>
      val unpickle = new Unpickler
      val pickle = new Pickler
      iter.map { row =>
        row match {
           //case obj: String => obj.split(",").map(UnEscapeString(_))
           case everythingElse: Array[Byte] => pickle.dumps(unpickle.loads(everythingElse) match {
             case str: String => UnEscapeString(str) 
           })
        }
      }
     
     }.toJavaRDD()
  } 
   
 def toJavaStringOfValues(jRDD: RDD[Any]): JavaRDD[String] = {

  jRDD.mapPartitions { iter =>
      iter.map { row =>
        row match {
          case obj: Seq[Any] => obj.map { item => 
            item match {
              case str: String => EscapeString(str)
              case others: Any => others
            }
          }.mkString(",")
        }   
      }   
    }.toJavaRDD()
  }
  def splitUnEscapeJavaRDD(jRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[String]] = {
     jRDD.rdd.mapPartitions { iter =>
      val unpickle = new Unpickler
      val pickle = new Pickler
      iter.map { row =>
        unpickle.loads(row) match {
           //case obj: String => obj.split(",").map(UnEscapeString(_))
           //case everything: Array[Byte] => pickle.dumps(unpickle.loads(everythingElse) match {
             case str: String => str.split(",").map(UnEscapeString(_))
           //}
        }
      }
     }.toJavaRDD()

  }
   
  def pythonToJava(pyRDD: JavaRDD[Any]): JavaRDD[Object] = {
    pyRDD.rdd.mapPartitions { iter =>
      val unpickle = new Unpickler
      iter.map { row =>
        row match { 
          case obj: String => obj
          case everythingElse: Array[Byte] => unpickle.loads(everythingElse)
        }
      }
    }.toJavaRDD()
  }

}
