package gg;

import java.util.List;

public class ElementOrEvent<T> {

	public byte subPartitionId; // az input operator melyik physical instance-erol jott
	public T element;
	public Event event;

	public byte splitId; // A 0 a normal (nem-cond) output!

	public ElementOrEvent(byte subPartitionId, T element) {
		this.subPartitionId = subPartitionId;
		this.element = element;
	}

	public ElementOrEvent(byte subPartitionId, Event event) {
		this.subPartitionId = subPartitionId;
		this.event = event;
	}

	// Bag start or end
	public static class Event {

		public enum Type {START, END}

		public Type type;
		public int cflSize;

		public Event(Type type, int cflSize) {
			this.type = type;
			this.cflSize = cflSize;
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
				'}';
	}
}
