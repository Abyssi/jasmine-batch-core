package operators.model

/**
  * Mean Counter class
  * @param values
  */
class MeanCounter(values: TraversableOnce[Double]) extends Serializable {
  private var n: Long = 0 // Running count of our values
  private var mu: Double = 0 // Running mean of our values

  merge(values)

  /** Initialize MeanCounter */
  def this() = this(Nil)

  /** Add values into MeanCounter updating statistics */
  def merge(values: TraversableOnce[Double]): MeanCounter = {
    values.foreach(v => merge(v))
    this
  }

  /** Add value into MeanCounter updating statistics */
  def merge(value: Double): MeanCounter = {
    val delta = value - mu
    n += 1
    mu += delta / n
    this
  }

  /** Merge another MeanCounter adding up statistics */
  def merge(other: MeanCounter): MeanCounter = {
    if (other == this) {
      merge(other.copy()) // Avoid overwriting fields in a weird order
    } else {
      if (n == 0) {
        mu = other.mu
        n = other.n
      } else if (other.n != 0) {
        val delta = other.mu - mu
        if (other.n * 10 < n) {
          mu = mu + (delta * other.n) / (n + other.n)
        } else if (n * 10 < other.n) {
          mu = other.mu - (delta * n) / (n + other.n)
        } else {
          mu = (mu * n + other.mu * other.n) / (n + other.n)
        }
        n += other.n
      }
      this
    }
  }

  /** Clone MeanCounter */
  def copy(): MeanCounter = {
    val other = new MeanCounter
    other.n = n
    other.mu = mu
    other
  }

  def sum: Double = n * mu

  override def toString: String = {
    "(count: %d, mean: %f)".format(count, mean)
  }

  def count: Long = n

  def mean: Double = mu
}

object MeanCounter {
  def apply(values: TraversableOnce[Double]): MeanCounter = new MeanCounter(values)
  def apply(values: Double*): MeanCounter = new MeanCounter(values)
}
