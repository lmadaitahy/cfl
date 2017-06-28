package gg;

//TODO: check what serializer is being used. If Kryo, then I should provide a custom one instead.
// (There might be a problem with the element field being type T)

import org.apache.flink.streaming.api.CanForceFlush;

public class ElementOrEvent<T> implements CanForceFlush {

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
}
