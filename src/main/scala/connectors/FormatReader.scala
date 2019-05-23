package connectors

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object FormatReader {
  def apply[O: ClassTag](format: String, parser: Parser[O]): FormatReader[O] = {
    format match {
      case "avro" => new AvroReader(parser)
      case "csv" => new CsvReader(parser)
      case _ => throw new IllegalArgumentException
    }
  }
}

trait FormatReader[O] extends Serializable  {
  var parser: Parser[O]

  def load(spark: SparkContext, path: String): RDD[O]
}
