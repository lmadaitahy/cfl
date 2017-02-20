package gg.partitioners;

import gg.ElementOrEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public abstract class EventBroadcast<T> extends StreamPartitioner<ElementOrEvent<T>> {

	private int[] singArr = new int[1];

	private int[] broadArr;
	private boolean set;
	private int setNumber;

	@Override
	public int[] selectChannels
			(SerializationDelegate<StreamRecord<ElementOrEvent<T>>> streamRecordSerializationDelegate,
			 int numberOfOutputChannels) {
		ElementOrEvent<T> ee = streamRecordSerializationDelegate.getInstance().getValue();
		if(ee.event != null) {

			if (set && setNumber == numberOfOutputChannels) {
				return broadArr;
			} else {
				this.broadArr = new int[numberOfOutputChannels];
				for (int i = 0; i < numberOfOutputChannels; i++) {
					broadArr[i] = i;
				}
				set = true;
				setNumber = numberOfOutputChannels;
				return broadArr;
			}

		} else {

			assert ee.element != null;
			singArr[0] = selectForElement(ee.element, numberOfOutputChannels);
			return singArr;

		}
	}

	protected abstract int selectForElement(T elem, int numberOfOutputChannels);

	@Override
	public StreamPartitioner<ElementOrEvent<T>> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "EVENT-BROADCAST";
	}
}
