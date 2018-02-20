package eu.stratosphere.labyrinth.partitioners

object ScalaParts {

	def forward[T](targetPara: Int): Forward[T] = {
		new Forward[T](targetPara)
	}

}
