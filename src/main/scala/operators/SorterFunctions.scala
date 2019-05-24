package operators

import operators.model.{BoundedPriorityQueue, PriorityQueue}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SorterFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Serializable {
  /**
    * top by key method
    * @param num
    * @param ord
    * @return
    */
  def topByKey(num: Int)(implicit ord: Ordering[V]): RDD[(K, (Int, V))] = {
    self.aggregateByKey(new BoundedPriorityQueue[V](num)(ord))(
      seqOp = (queue, item) => {
        queue += item
      },
      combOp = (queue1, queue2) => {
        queue1 ++= queue2
      }
    ).mapValues(_.toArray.sorted(ord.reverse)) // This is a min-heap, so we reverse the order.
      .flatMapValues(_.zipWithIndex.map { case (v, i) => (i + 1, v) }) // flatmap values to (index, value)
  }

  /**
    * sort by key method
    * @param ord
    * @return
    */
  def sortByKey()(implicit ord: Ordering[V]): RDD[(K, (Int, V))] = {
    self.aggregateByKey(new PriorityQueue[V]()(ord))(
      seqOp = (queue, item) => {
        queue += item
      },
      combOp = (queue1, queue2) => {
        queue1 ++= queue2
      }
    ).mapValues(_.toArray.sorted(ord.reverse)) // This is a min-heap, so we reverse the order.
      .flatMapValues(_.zipWithIndex.map { case (v, i) => (i + 1, v) }) // flatmap values to (index, value)
  }
}

/**
  * sorter functions
  */
object SorterFunctions {
  def fromPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SorterFunctions[K, V] =
    new SorterFunctions[K, V](rdd)
}
