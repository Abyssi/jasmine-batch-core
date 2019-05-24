package utils

import org.apache.spark.rdd.RDD

/**
  * Profiling Utils
  */
object ProfilingUtils {
  def timeRDD[O](rdd: RDD[O], id: String): RDD[O] = {
    val start = System.nanoTime()
    rdd.setName(id).first()
    val end = System.nanoTime()

    println(s"$id - ${(end - start) / 1e09} sec")
    rdd
  }
}
