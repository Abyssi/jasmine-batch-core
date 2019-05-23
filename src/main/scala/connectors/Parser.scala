package connectors

import org.apache.avro.generic.GenericRecord

trait Parser[O] extends Serializable  {
  def parse(input: Array[String]): O
  def parse(input: GenericRecord): O
}