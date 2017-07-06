package gg;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.CanForceFlush;

import java.io.IOException;
import java.io.Serializable;

public class ElementOrEvent<T> implements Serializable, CanForceFlush {

	public short subPartitionId; // az input operator melyik physical instance-erol jott
	public T element;
	public Event event;

	public byte splitId; // Ementen kell majd splittelni a conditional outputokhoz

	public byte logicalInputId = -1;

	public short targetPart; // A FlinkPartitioner ezt hasznalja

	// ! Vigyazni, hogy ha ide folveszek vmi field-et, akkor azt beirjam a copy-ba is !

	public ElementOrEvent() {}

	public ElementOrEvent(short subPartitionId, T element, byte splitId, short targetPart) {
		this.subPartitionId = subPartitionId;
		this.element = element;
		this.splitId = splitId;
		this.targetPart = targetPart;
	}

	public ElementOrEvent<T> replace(short subPartitionId, T element, byte splitId, short targetPart) {
		this.subPartitionId = subPartitionId;
		this.element = element;
		this.splitId = splitId;
		this.targetPart = targetPart;
		return this;
	}

	public ElementOrEvent(short subPartitionId, Event event, byte splitId, short targetPart) {
		this.subPartitionId = subPartitionId;
		this.event = event;
		this.splitId = splitId;
		this.targetPart = targetPart;
	}

	public ElementOrEvent<T> copy() {
		ElementOrEvent<T> c = new ElementOrEvent<T>();
		c.subPartitionId = subPartitionId;
		c.element = element;
		c.event = event;
		c.splitId = splitId;
		c.logicalInputId = logicalInputId;
		c.targetPart = targetPart;
		return c;
	}

	@Override
	public boolean shouldFlush() {
		return event != null && event.type == Event.Type.END;
	}

	// Bag start or end
	// Note: this should be immutable
	public static class Event {

		public enum Type {START, END}

		public Type type;
		public short assumedTargetPara;
		public BagID bagID;

		public Event() {}

		public Event(Type type, short assumedTargetPara, BagID bagID) {
			this.type = type;
			this.assumedTargetPara = assumedTargetPara;
			this.bagID = bagID;
		}

		@Override
		public String toString() {
			return "Event{" +
					"type=" + type +
					", assumedTargetPara=" + assumedTargetPara +
					", bagID=" + bagID +
					'}';
		}
	}

	@Override
	public String toString() {
		return "ElementOrEvent{" +
				"subPartitionId=" + subPartitionId +
				", element=" + element +
				", event=" + event +
				", splitId=" + splitId +
				", logicalInputId=" + logicalInputId +
				", targetPart=" + targetPart +
				'}';
	}

	// ------------------------- Serializers -------------------------

//	public static final class ElementOrEventSerializer extends TypeSerializer<ElementOrEvent> {
//		@Override
//		public boolean isImmutableType() {
//			return false;
//		}
//
//		@Override
//		public TypeSerializer<ElementOrEvent> duplicate() {
//			return this;
//		}
//
//		@Override
//		public ElementOrEvent createInstance() {
//			return new ElementOrEvent();
//		}
//
//		@Override
//		public ElementOrEvent copy(ElementOrEvent from) {
//			return from.copy();
//		}
//
//		@Override
//		public ElementOrEvent copy(ElementOrEvent from, ElementOrEvent reuse) {
//			return from.copy();
//		}
//
//		@Override
//		public int getLength() {
//			//todo
//		}
//
//		@Override
//		public void serialize(ElementOrEvent record, DataOutputView target) throws IOException {
////todo
//		}
//
//		@Override
//		public ElementOrEvent deserialize(DataInputView source) throws IOException {
//			//todo
//		}
//
//		@Override
//		public ElementOrEvent deserialize(ElementOrEvent reuse, DataInputView source) throws IOException {
//			//todo
//		}
//
//		@Override
//		public void copy(DataInputView source, DataOutputView target) throws IOException {
////todo
//		}
//
//		@Override
//		public boolean equals(Object obj) {
//			//todo
//		}
//
//		@Override
//		public boolean canEqual(Object obj) {
//			//todo
//		}
//
//		@Override
//		public int hashCode() {
//			//todo
//		}
//	}
}
