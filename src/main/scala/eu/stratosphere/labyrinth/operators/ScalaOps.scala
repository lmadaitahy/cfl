package eu.stratosphere.labyrinth.operators

import java.util
import java.util.HashMap

import eu.stratosphere.labyrinth.BagOperatorOutputCollector
import eu.stratosphere.labyrinth.partitioners.Forward
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.tuple.Tuple2

object ScalaOps {

	def map[IN, OUT](f: (IN => OUT)): FlatMap[IN, OUT] = {

		new FlatMap[IN, OUT]() {
			override def pushInElement(e: IN, logicalInputId: Int): Unit = {
				super.pushInElement(e, logicalInputId)
				out.collectElement(f(e))
			}
		}
	}

	def flatMap[IN, OUT](f: (IN, BagOperatorOutputCollector[OUT]) => Unit): FlatMap[IN, OUT] = {

		new FlatMap[IN, OUT]() {
			override def pushInElement(e: IN, logicalInputId: Int): Unit = {
				super.pushInElement(e, logicalInputId)
				f(e, out)
			}
		}
	}

	def foldGroup[K,IN,OUT](keyExtractor: IN => K, i: IN => OUT, f: (OUT, OUT) => OUT): FoldGroup[K,IN,OUT] = {

		new FoldGroup[K, IN, OUT]() {

			override protected def keyExtr(e: IN): K = keyExtractor(e)

			override def openOutBag(): Unit = {
				super.openOutBag()
				hm = new util.HashMap[K, OUT]
			}

			// TODO check crash in clickcountdiffsscala
			override def pushInElement(e: IN, logicalInputId: Int): Unit = {
				super.pushInElement(e, logicalInputId)
				val key = keyExtr(e)
				val g = hm.get(key)
				if (g == null) {
					hm.put(key, i(e))
				} else {
					hm.replace(key, f(g, i(e)))
				}
			}

			override def closeInBag(inputId: Int): Unit = {
				super.closeInBag(inputId)

				import scala.collection.JavaConversions._

				for (e <- hm.entrySet) {
					out.collectElement(e.getValue)
				}
				hm = null
				out.closeBag()
			}
		}
	}

	def reduceGroup[K,A](keyExtractor: A => K, f: (A, A) => A): FoldGroup[K, A, A] = {
		foldGroup(keyExtractor, (x:A) => x, f)
	}

	def joinGeneric[IN, K](keyExtractor: IN => K): JoinGeneric[IN, K] = {
		new JoinGeneric[IN, K] {
			override protected def keyExtr(e: IN): K = keyExtractor(e)
		}
	}

	def singletonBagOperator[IN, OUT](f: IN => OUT): SingletonBagOperator[IN, OUT] = {
		new SingletonBagOperator[IN, OUT] {
			override def pushInElement(e: IN, logicalInputId: Int): Unit = {
				super.pushInElement(e, logicalInputId)
				out.collectElement(f(e))
			}
		}
	}

	def union[T](): Union[T] = {
		new Union[T]
	}
}
