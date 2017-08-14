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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Note: Only one job can be built in a program. (Because of the labyNodes list.)
 * @param <IN>
 * @param <OUT>
 */
public class LabyNode<IN, OUT> extends AbstractLabyNode<IN, OUT> {

    // --- Initialized in ctor ---

    private final BagOperatorHost<IN, OUT> bagOpHost; // This is null in LabySource

    private final Partitioner<IN> inputPartitioner;

    // --- Initialized in builder methods (addInput, setParallelism) ---

    private List<Input> inputs = new ArrayList<>();

    private int parallelism = -1;

    // --- Initialized in translate ---

    private DataStream<ElementOrEvent<OUT>> flinkStream;

    // --------------------------------

    public LabyNode(String name, BagOperator<IN,OUT> op, int bbId, Partitioner<IN> inputPartitioner, TypeSerializer<IN> inSer) {
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
        inputs.add(new Input(inputLabyNode, splitID));
        return this;
    }

    // This overload is for adding a LabySource as input
    public LabyNode<IN, OUT> addInput(LabySource<IN> inputLabySource) {
        bagOpHost.addInput(inputs.size(), inputLabySource.bbId, true, inputLabySource.opID);
        inputLabySource.bagify.setPartitioner(inputPartitioner);
        inputs.add(new Input(inputLabySource, 0));
        return this;
    }

    @Override
    protected List<AbstractLabyNode<?, IN>> getInputNodes() {
        List<AbstractLabyNode<?, IN>> ret = new ArrayList<>();
        for (Input in: inputs) {
            ret.add(in.node);
        }
        return ret;
    }

    @Override
    public DataStream<ElementOrEvent<OUT>> getFlinkStream() {
        return flinkStream;
    }

    public LabyNode<IN, OUT> setParallelism(int parallelism) {
        this.parallelism = parallelism;
        assert !(parallelism == 1 && !(inputPartitioner instanceof Always0));
        return this;
    }

    public static void translateAll() {
        for (AbstractLabyNode<?, ?> ln: labyNodes) {
            ln.translate();
        }

        int totalPara = 0;
        for (AbstractLabyNode<?, ?> ln: labyNodes) {
            if (ln instanceof LabyNode) {
                totalPara += ln.getFlinkStream().getParallelism();
            }
        }
        CFLConfig.getInstance().setNumToSubscribe(totalPara);
    }

    @Override
    protected void translate() {

        // We need an iteration if we have at least one such input that hasn't been translated yet
        boolean needIter = false;
        for (AbstractLabyNode<?, ?> inp: getInputNodes()) {
            if (inp.getFlinkStream() == null) {
                needIter = true;
            }
        }

        // Determine input parallelism (and make sure all inputs have the same para)
        Integer inputPara = null;
        List<AbstractLabyNode<?, IN>> inpNodes = getInputNodes();
        assert !inpNodes.isEmpty();
        for (AbstractLabyNode<?, IN> inp : inpNodes) {
            assert inputPara == null || inputPara.equals(inp.getFlinkStream().getParallelism());
            inputPara = inp.getFlinkStream().getParallelism();
        }
        bagOpHost.setInputPara(inputPara);

        if (!needIter) {
            boolean needUnion = inputs.size() > 1;

            DataStream<ElementOrEvent<IN>> inputStream = null;
            if (!needUnion) {
                inputStream = inpNodes.get(0).getFlinkStream();
            } else {
                int i = 0;
                for (Input inp : inputs) {
                    DataStream<ElementOrEvent<IN>> inpStreamFilledAndSelected;
                    if (inp.node.getFlinkStream() instanceof SplitStream) {
                        inpStreamFilledAndSelected = ((SplitStream<ElementOrEvent<IN>>) inp.node.getFlinkStream())
                                .select(((Integer)inp.splitID).toString());
                    } else {
                        inpStreamFilledAndSelected = inp.node.getFlinkStream();
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

            assert inputStream != null;
            SingleOutputStreamOperator<ElementOrEvent<OUT>> tmpFlinkStream =
                    inputStream.transform(bagOpHost.name, Util.tpe(), bagOpHost);

            if (parallelism != -1) {
                tmpFlinkStream.setParallelism(parallelism);
            }

            tmpFlinkStream = tmpFlinkStream.setConnectionType(new FlinkPartitioner<>()); // this has to be after setting the para

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
            //throw new NotImplementedException();

            assert inputs.size() > 1;

        }
    }

    private class Input {

        public AbstractLabyNode<?, IN> node;
        public int splitID;
        public boolean backEdge;

        public Input(AbstractLabyNode<?, IN> node, int splitID) {
            this.node = node;
            this.splitID = splitID;
            this.backEdge = false;
        }
    }
}
