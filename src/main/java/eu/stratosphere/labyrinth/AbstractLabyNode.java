package eu.stratosphere.labyrinth;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;

abstract class AbstractLabyNode<OUT> {

    // Maybe I should factor this out into a LabyrinthEnvironment?
    protected static List<AbstractLabyNode<?>> labyNodes = new ArrayList<>(); // all BagStreams

    abstract protected DataStream<ElementOrEvent<OUT>> getFlinkStream();

    abstract protected void translate();
}
