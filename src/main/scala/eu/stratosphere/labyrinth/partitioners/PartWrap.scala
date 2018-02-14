package eu.stratosphere.labyrinth.partitioners

object PartWrap {

	def forward[T](targetPara: Int): Forward[T] = {
		new Forward[T](targetPara)
	}

}
