package operators

import operators.model.{BoundedPriorityQueue, PriorityQueue}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SorterFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Serializable {
  /**
    * Returns the top k (largest) elements for each key from this RDD as defined by the specified
    * implicit Ordering[T].
    * If the number of elements for a certain key is less than k, all of them will be returned.
    *
    * @param num k, the number of top elements to return
    * @param ord the implicit ordering for T
    * @return an RDD that contains the top k values for each key
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

object SorterFunctions {
  def fromPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SorterFunctions[K, V] =
    new SorterFunctions[K, V](rdd)
}
