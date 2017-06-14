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

	public short part; // A FlinkPartitioner ezt adja vissza

	public ElementOrEvent() {}

	public ElementOrEvent(short subPartitionId, T element, byte splitId, short part) {
		this.subPartitionId = subPartitionId;
		this.element = element;
		this.splitId = splitId;
		this.part = part;
	}

	public ElementOrEvent(short subPartitionId, Event event, byte splitId, short part) {
		this.subPartitionId = subPartitionId;
		this.event = event;
		this.splitId = splitId;
		this.part = part;
	}

	public ElementOrEvent<T> copy() {
		ElementOrEvent<T> c = new ElementOrEvent<T>();
		c.subPartitionId = subPartitionId;
		c.element = element;
		c.event = event;
		c.splitId = splitId;
		c.logicalInputId = logicalInputId;
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
		public int cflSize;
		public short assumedTargetPara;
		public int opID;

		public Event() {}

		public Event(Type type, int cflSize, short assumedTargetPara, int opID) {
			this.type = type;
			this.cflSize = cflSize;
			this.assumedTargetPara = assumedTargetPara;
			this.opID = opID;
		}

		@Override
		public String toString() {
			return "Event{" +
					"type=" + type +
					", cflSize=" + cflSize +
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
				'}';
	}
}
