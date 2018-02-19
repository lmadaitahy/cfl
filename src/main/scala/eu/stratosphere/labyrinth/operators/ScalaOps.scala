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

	def foldGroup[K,A,B](i: A => B, f: (B, B) => B): FoldGroup[K,A,B] = {

		new FoldGroup[K, A, B]() {

			override def openOutBag(): Unit = {
				super.openOutBag()
				hm = new util.HashMap[K, B]
			}

			override def pushInElement(e: tuple.Tuple2[K, A], logicalInputId: Int): Unit = {
				super.pushInElement(e, logicalInputId)
				val g = hm.get(e.f0)
				if (g == null) {
					hm.put(e.f0, i(e.f1))
				} else {
					hm.replace(e.f0, f(g, i(e.f1)))
				}
			}

			override def closeInBag(inputId: Int): Unit = {
				super.closeInBag(inputId)

				import scala.collection.JavaConversions._

				for (e <- hm.entrySet) {
					out.collectElement(Tuple2.of(e.getKey, e.getValue))
				}
				out.closeBag()
				hm = null
			}
		}
	}

	def reduceGroup[K,A](f: (A, A) => A): FoldGroup[K, A, A] = {
		foldGroup((x:A) => x, f)
	}

	def joinGeneric[IN, K](kexExtractor: IN => K): JoinGeneric[IN, K] = {
		new JoinGeneric[IN, K] {
			override protected def keyExtr(e: IN): K = kexExtractor(e)
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
