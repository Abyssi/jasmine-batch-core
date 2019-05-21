package operators.model

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}

import scala.collection.JavaConverters._
import scala.collection.generic.Growable

/**
  * Priority queue. This class wraps the original PriorityQueue
  * class and modifies it such that only the top K elements are retained.
  * The top K elements are defined by an implicit Ordering[A].
  */
class PriorityQueue[A]()(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  private val underlying = new JPriorityQueue[A](ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  def poll(): A = {
    underlying.poll()
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach {
      this += _
    }
    this
  }

  override def +=(elem: A): this.type = {
    underlying.offer(elem)
    this
  }

  override def size: Int = underlying.size

  override def clear() {
    underlying.clear()
  }
}
