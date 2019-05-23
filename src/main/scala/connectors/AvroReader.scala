package connectors

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class AvroReader[O: ClassTag](var _parser: Parser[O]) extends FormatReader[O] {

  override var parser: Parser[O] = _parser
  override def load(spark: SparkContext, path: String): RDD[O] = {
    spark
      .hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
      .map(item => parser.parse(item._1.datum))
  }

}
