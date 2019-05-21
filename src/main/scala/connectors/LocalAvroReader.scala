package connectors

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LocalAvroReader {

  def load(spark: SparkContext, path: String): RDD[GenericRecord] = {
    spark.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)
      .map(item => item._1.datum)
  }
}
