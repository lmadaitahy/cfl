package gg;


import gg.operators.Identity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


public class PhiNode2<T> extends BagOperatorHost<T,T> implements Serializable {

	protected static final Logger LOG = LoggerFactory.getLogger(PhiNode2.class);

	public PhiNode2(int bbId) {
		super(new Identity<T>(), bbId);
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
					assert inp.inputCFLSize == -1;
					inp.inputCFLSize = i + 1;
					activateLogicalInput(j);
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
