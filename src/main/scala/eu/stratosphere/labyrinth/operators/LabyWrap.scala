package eu.stratosphere.labyrinth.operators

import java.util
import java.util.HashMap

import eu.stratosphere.labyrinth.BagOperatorOutputCollector
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.tuple.Tuple2

object LabyWrap {

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

	def groupBy[LEFT,RIGHT](f: (RIGHT, RIGHT) => RIGHT): GroupBy[LEFT,RIGHT] = {

		new GroupBy[LEFT,RIGHT]() {
			override def openOutBag(): Unit = {
				super.openOutBag()
				hm = new util.HashMap[LEFT, RIGHT]
			}

			override def pushInElement(e: tuple.Tuple2[LEFT, RIGHT], logicalInputId: Int): Unit = {
				super.pushInElement(e, logicalInputId)
				val g = hm.get(e.f0)
				if (g == null) {
					hm.put(e.f0, e.f1)
				} else {
					hm.replace(e.f0, f(g, e.f1))
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
}
