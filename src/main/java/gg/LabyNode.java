package gg;

import gg.operators.BagOperator;
import gg.partitioners.Always0;
import gg.partitioners.FlinkPartitioner;
import gg.partitioners.Partitioner;
import gg.util.LogicalInputIdFiller;
import gg.util.Util;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;

import java.util.ArrayList;
import java.util.List;


/**
 * Note: Only one job can be built in a program. (Because of the labyNodes list.)
 * @param <IN>
 * @param <OUT>
 */
public class LabyNode<IN, OUT> extends AbstractLabyNode<IN, OUT> {

    // --- Initialized in ctor ---

    private final BagOperatorHost<IN, OUT> bagOpHost; // This is null in LabySource

    private final Partitioner<IN> inputPartitioner;

    private final TypeInformation<ElementOrEvent<OUT>> typeInfo;

    // --- Initialized in builder methods (addInput, setParallelism) ---

    private List<Input> inputs = new ArrayList<>();

    private int parallelism = -1;

    // --- Initialized in translate ---

    private DataStream<ElementOrEvent<OUT>> flinkStream;
    private IterativeStream<ElementOrEvent<OUT>> iterativeStream; // This is filled only when we are an IterativeStream

    // --------------------------------

    private List<Close> closeTo = new ArrayList<>();


    public LabyNode(String name, BagOperator<IN,OUT> op, int bbId, Partitioner<IN> inputPartitioner, TypeSerializer<IN> inSer, TypeInformation<ElementOrEvent<OUT>> typeInfo) {
        this.inputPartitioner = inputPartitioner;
        this.bagOpHost = new BagOperatorHost<>(op, bbId, labyNodes.size(), inSer);
        this.bagOpHost.setName(name);
        this.typeInfo = typeInfo;
        labyNodes.add(this);
    }

    public static <T> LabyNode<T, T> phi(String name, int bbId, Partitioner<T> inputPartitioner, TypeSerializer<T> inSer, TypeInformation<ElementOrEvent<T>> typeInfo) {
        return new LabyNode<>(name, bbId, inputPartitioner, inSer, typeInfo);
    }

    private LabyNode(String name, int bbId, Partitioner<IN> inputPartitioner, TypeSerializer<IN> inSer, TypeInformation<ElementOrEvent<OUT>> typeInfo) {
        this.inputPartitioner = inputPartitioner;
        this.bagOpHost = (BagOperatorHost<IN, OUT>) new PhiNode<>(bbId, labyNodes.size(), inSer);
        this.bagOpHost.setName(name);
        this.typeInfo = typeInfo;
        labyNodes.add(this);
    }

    // Az insideBlock ugy ertve, hogy ugyanabban a blockban van, es elotte a kodban.
    // Azaz ha ugyanabban a blockban van, de utana, akkor "block-ot lepunk".
    public LabyNode<IN, OUT> addInput(LabyNode<?, IN> inputLabyNode, boolean insideBlock, boolean condOut) {
        assert !(insideBlock && condOut); // This case is impossible, right?
        bagOpHost.addInput(inputs.size(), inputLabyNode.bagOpHost.bbId, insideBlock, inputLabyNode.bagOpHost.opID);
        int splitID = inputLabyNode.bagOpHost.out(bagOpHost.bbId, !condOut, inputPartitioner);
        inputs.add(new Input(inputLabyNode, splitID, inputs.size()));
        return this;
    }

    // This overload is when your operator needs different partitioners on different inputs
    public LabyNode<IN, OUT> addInput(LabyNode<?, IN> inputLabyNode, boolean insideBlock, boolean condOut, Partitioner<IN> partitioner) {
        assert inputPartitioner == null;
        assert !(insideBlock && condOut); // This case is impossible, right?
        bagOpHost.addInput(inputs.size(), inputLabyNode.bagOpHost.bbId, insideBlock, inputLabyNode.bagOpHost.opID);
        int splitID = inputLabyNode.bagOpHost.out(bagOpHost.bbId, !condOut, partitioner);
        inputs.add(new Input(inputLabyNode, splitID, inputs.size()));
        return this;
    }

    // This overload is for adding a LabySource as input
    // (Does not support condOut at the moment, which would need full-blown BagOperatorHost.Out functionality in Bagify)
    public LabyNode<IN, OUT> addInput(LabySource<IN> inputLabySource, boolean insideBlock) {
        bagOpHost.addInput(inputs.size(), inputLabySource.bbId, insideBlock, inputLabySource.opID);
        inputLabySource.bagify.setPartitioner(inputPartitioner);
        inputs.add(new Input(inputLabySource, 0, inputs.size()));
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
    @SuppressWarnings("unchecked")
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
            if (inp.getFlinkStream() != null) {
                assert inputPara == null || inputPara.equals(inp.getFlinkStream().getParallelism());
                inputPara = inp.getFlinkStream().getParallelism();
            }
        }
        bagOpHost.setInputPara(inputPara);

        DataStream<ElementOrEvent<IN>> inputStream = null;

        if (!needIter) {
            boolean needUnion = inputs.size() > 1;

            if (!needUnion) {
                Input inp = inputs.get(0);
                if (inp.node.getFlinkStream() instanceof SplitStream) {
                    inputStream = ((SplitStream<ElementOrEvent<IN>>) inp.node.getFlinkStream())
                            .select(((Integer)inp.splitID).toString());
                } else {
                    inputStream = inp.node.getFlinkStream();
                }
            } else {
                int i = 0;
                for (Input inpI : inputs) {
                    DataStream<ElementOrEvent<IN>> inpIFilledAndSelected;
                    if (inpI.node.getFlinkStream() instanceof SplitStream) {
                        inpIFilledAndSelected = ((SplitStream<ElementOrEvent<IN>>) inpI.node.getFlinkStream())
                                .select(((Integer)inpI.splitID).toString());
                    } else {
                        inpIFilledAndSelected = inpI.node.getFlinkStream();
                    }
                    inpIFilledAndSelected = inpIFilledAndSelected.map(new LogicalInputIdFiller<>(i));
                    if (inputStream == null) {
                        inputStream = inpIFilledAndSelected;
                    } else {
                        inputStream = inputStream.union(inpIFilledAndSelected);
                    }
                    i++;
                }
            }
        } else {
            assert inputs.size() > 1;

            // Which input will be the forward edge?
            // (We assume for the moment that we have only one such input that should be a forward edge,
            // i.e., we won't need union before .iterate)
            Input forward = null;
            for (Input inp: inputs) {
                if (inp.node.getFlinkStream() != null) {
                    assert forward == null;
                    forward = inp;
                }
            }
            assert forward != null;

            inputStream = forward.node.getFlinkStream();

            inputStream = inputStream
                    .map(new LogicalInputIdFiller<>(forward.index))
                    .iterate(1000000000L);
            iterativeStream = (IterativeStream)inputStream;

            assert bagOpHost instanceof PhiNode;

            // Save the info to backedge inputs that they will need to do closeWith to us
            for (Input inp: inputs) {
                if (inp.node.getFlinkStream() == null) {
                    assert inp.node instanceof LabyNode;
                    ((LabyNode)inp.node).closeTo.add(new Close(iterativeStream, inp.splitID, inp.index));
                }
            }
        }

        assert inputStream != null;
        SingleOutputStreamOperator<ElementOrEvent<OUT>> tmpFlinkStream =
                inputStream.transform(bagOpHost.name, Util.tpe(), bagOpHost);

        if (parallelism != -1) {
            tmpFlinkStream.setParallelism(parallelism);
        }
        assert inputPartitioner == null || inputPartitioner.targetPara == tmpFlinkStream.getParallelism();

        tmpFlinkStream.returns(typeInfo);
        tmpFlinkStream = tmpFlinkStream.setConnectionType(new FlinkPartitioner<>()); // this has to be after setting the para

        flinkStream = tmpFlinkStream;

        boolean needSplit = false;
        if (bagOpHost.outs.size() > 1) {
//            // If there are more than one outs we still want to check whether at least one of them is conditional.
//            for (BagOperatorHost.Out out : bagOpHost.outs) {
//                if (!out.normal) {
//                    needSplit = true;
//                    break;
//                }
//            }
            needSplit = true;  //todo: el kene donteni, hogy ez hogy legyen. Az a baj, hogy itt mar tul keso eldonteni, mert ha az addInput-ban mar tobb out-ot hozunk letre, akkor mindenkeppen splittelni kell. Viszont ez ugyebar igy esetleg lassithat kicsit a regi jobokhoz kepest, bar remelhetoleg nem sokat. Le kene majd merni, hogy mennyit.
                // De varjunk: el tudjuk donteni az addInput-ban! Szoval a needSplit member variable lenne, amit az addInputban settelnenk, es minden nem conditional out egyetlen out-ban lenne. (Csak le kell ellenorizni, hogy a splitteles logikaja tudja-e ezt a setupot kezelni megfeleloen)
        }
        if (needSplit) {
            flinkStream = flinkStream.split(new CondOutputSelector<>());
        }

        // Attend to closeTo
        for (Close c: closeTo) {
            DataStream<ElementOrEvent<OUT>> maybeSelected = flinkStream;
            if (flinkStream instanceof SplitStream) {
                maybeSelected = ((SplitStream<ElementOrEvent<OUT>>)maybeSelected).select(((Integer)c.splitID).toString());
            }
            DataStream<ElementOrEvent<OUT>> toCloseWith = maybeSelected.map(new LogicalInputIdFiller<>(c.index));
            try {
                c.iterativeStream.closeWith(toCloseWith);
            } catch (UnsupportedOperationException ex) {
                // Introduce dummy edge from the IterativeStream to avoid this issue
                c.iterativeStream.closeWith(c.iterativeStream.filter(new FilterFunction() {
                    @Override
                    public boolean filter(Object value) throws Exception {
                        return false;
                    }
                }).union(toCloseWith));
            }
        }
    }


    private class Input {

        public AbstractLabyNode<?, IN> node;
        public int splitID;
        public int index;
        public boolean backEdge;

        public Input(AbstractLabyNode<?, IN> node, int splitID, int index) {
            this.node = node;
            this.splitID = splitID;
            this.index = index;
            this.backEdge = false;
        }
    }

    private class Close {

        IterativeStream iterativeStream;
        int splitID;
        int index;

        public Close(IterativeStream iterativeStream, int splitID, int index) {
            this.iterativeStream = iterativeStream;
            this.splitID = splitID;
            this.index = index;
        }
    }


    public static void printOperatorIDNameMapping() {
        for (AbstractLabyNode<?, ?> ln: labyNodes) {
            if (ln instanceof LabyNode) {
                BagOperatorHost<?, ?> boh = ((LabyNode) ln).bagOpHost;
                System.out.println(boh.opID + " " + boh.name);
            }
        }
    }
}
