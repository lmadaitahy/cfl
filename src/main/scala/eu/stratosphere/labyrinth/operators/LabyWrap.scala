package eu.stratosphere.labyrinth.operators

import eu.stratosphere.labyrinth.BagOperatorOutputCollector

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
}
