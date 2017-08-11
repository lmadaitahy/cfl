package gg;


import gg.operators.IdMap;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


public class PhiNode<T> extends BagOperatorHost<T,T, ElementOrEvent<T>> implements Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(PhiNode.class);

	public PhiNode(int bbId, int opID, TypeSerializer<T> inSer) {
		super(new IdMap<T>(), bbId, opID, inSer);
	}

	@Override
	protected void chooseLogicalInputs(int outCFLSize) {
		// figure out the input bag ID
		// Scan the CFL backwards, and find the first occurrence of a bbId of any of our logical inputs
		for (int i = outCFLSize - 2; ; i--) {
			assert i >= 0;
			int j = 0;
			boolean brk = false;
			for (Input inp: inputs) {
				if(inp.bbId == latestCFL.get(i)) {
					assert !brk; // because two logical inputs can't have the same basic block id
					activateLogicalInput(j, outCFLSize, i + 1);
					brk = true;
				}
				j++;
			}
			if(brk){
				break;
			}
		}
	}
}
