package gg;

import gg.operators.BagOperator;
import gg.partitioners.Always0;
import gg.partitioners.FlinkPartitioner;
import gg.partitioners.Partitioner;
import gg.util.LogicalInputIdFiller;
import gg.util.Util;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Note: Only one job can be built in a program. (Because of the labyNodes list.)
 * @param <IN>
 * @param <OUT>
 */
public class LabyNode<IN, OUT> {

    // --- Static ---

    // Maybe I should factor this out into a LabyrinthEnvironment?
    private static List<LabyNode<?, ?>> labyNodes; // all BagStreams

    // --- Initialized in ctor ---

    private final BagOperatorHost<IN, OUT, ElementOrEvent<IN>> bagOpHost; // This is null in LabySource

    private final Partitioner<IN> inputPartitioner;

    // --- Initialized in builder methods (addInput, setParallelism) ---

    private List<LabyNode<?, IN>> inputs = new ArrayList<>();
    private List<Integer> splitIDs = new ArrayList<>();

    private int parallelism = -1;

    // --- Initialized in translate ---

    private DataStream<ElementOrEvent<OUT>> flinkStream;

    // --------------------------------

    LabyNode(String name, BagOperator<IN,OUT> op, int bbId, Partitioner<IN> inputPartitioner, TypeSerializer<IN> inSer) {
        this.inputPartitioner = inputPartitioner;
        this.bagOpHost = new BagOperatorHost<>(op, bbId, labyNodes.size(), inSer);
        this.bagOpHost.setName(name);
        labyNodes.add(this);
    }

    // Az insideBlock ugy ertve, hogy ugyanabban a blockban van, es elotte a kodban.
    // Azaz ha ugyanabban a blockban van, de utana, akkor "block-ot lepunk".
    public LabyNode<IN, OUT> addInput(LabyNode<?, IN> inputLabyNode, boolean insideBlock, boolean condOut) {
        bagOpHost.addInput(inputs.size(), inputLabyNode.bagOpHost.bbId, insideBlock, inputLabyNode.bagOpHost.opID);
        int splitID = inputLabyNode.bagOpHost.out(bagOpHost.bbId, !condOut, inputPartitioner);
        inputs.add(inputLabyNode);
        splitIDs.add(splitID);
        return this;
    }

    public LabyNode<IN, OUT> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        assert !(parallelism == 1 && !(inputPartitioner instanceof Always0));
        return this;
    }

    public static void translateAll() {
        Set<LabyNode<?, ?>> seen = new HashSet<>();
        for (LabyNode<?, ?> ln: labyNodes) {

            seen.add(ln);

            boolean needIter = false;
            for (LabyNode<?, ?> inp: ln.inputs) {
                if (!seen.contains(inp)) {
                    needIter = true;
                }
            }

            ln.translate(needIter);
        }
    }

    private void translate(boolean needIter) {
        boolean needUnion = inputs.size() > 1;

        assert !(needIter && !needUnion);

        Integer inputPara = null;
        assert !inputs.isEmpty();
        for (LabyNode<?, IN> inp : inputs) {
            assert inputPara == null || inputPara.equals(inp.flinkStream.getParallelism());
            inputPara = inp.flinkStream.getParallelism();
        }
        bagOpHost.setInputPara(inputPara);

        if (!needIter) {
            DataStream<ElementOrEvent<IN>> inputStream = null;
            if (!needUnion) {
                inputStream = inputs.get(0).flinkStream;
            } else {
                int i = 0;
                for (LabyNode<?, IN> inp : inputs) {
                    DataStream<ElementOrEvent<IN>> inpStreamFilledAndSelected;
                    if (inp.flinkStream instanceof SplitStream) {
                        inpStreamFilledAndSelected = ((SplitStream<ElementOrEvent<IN>>) inp.flinkStream).select(splitIDs.get(i).toString());
                    } else {
                        inpStreamFilledAndSelected = inp.flinkStream;
                    }
                    inpStreamFilledAndSelected = inpStreamFilledAndSelected.map(new LogicalInputIdFiller<>(i));
                    if (inputStream == null) {
                        inputStream = inpStreamFilledAndSelected;
                    } else {
                        inputStream = inputStream.union(inpStreamFilledAndSelected);
                    }
                    i++;
                }
            }

            SingleOutputStreamOperator<ElementOrEvent<OUT>> tmpFlinkStream = inputStream
                    .transform(bagOpHost.name, Util.tpe(), bagOpHost)
                    .setConnectionType(new FlinkPartitioner<>());

            if (parallelism != -1) {
                tmpFlinkStream.setParallelism(parallelism);
            }

            flinkStream = tmpFlinkStream;

            boolean needSplit = false;
            if (bagOpHost.outs.size() > 1) {
                // If there are more than one outs we still want to check whether at least one of them is conditional.
                for (BagOperatorHost.Out out : bagOpHost.outs) {
                    if (!out.normal) {
                        needSplit = true;
                        break;
                    }
                }
            }
            if (needSplit) {
                assert parallelism == -1; // ez azert van, mert nem vagyok benne biztos, hogy a fentebbi para beallitas nem vesz-e itt el
                //   (amugy gondolom ezt meg lehetne oldani, hogy ne kelljen, ha fontos lenne)
                SplitStream<ElementOrEvent<OUT>> splitStream = flinkStream.split(new CondOutputSelector<>());
                flinkStream = new SingleOutputStreamOperator<>(splitStream.getExecutionEnvironment(), splitStream.getTransformation());
            }
        } else {
            //todo
            throw new NotImplementedException();
        }
    }
}
