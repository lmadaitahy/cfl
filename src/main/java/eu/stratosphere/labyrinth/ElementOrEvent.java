package eu.stratosphere.labyrinth;

import eu.stratosphere.labyrinth.BagID;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
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

		public static final Type[] enumConsts = Type.class.getEnumConstants();
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

	public static final class ElementOrEventSerializerFactory implements PojoTypeInfo.CustomSerializerFactory {
		@Override
		public <C> TypeSerializer<C> get(PojoTypeInfo<C> tpe) {
			final int elemFieldInd = 0;
			assert tpe.getFieldNames()[elemFieldInd].equals("element");
			TypeInformation elemTpe = tpe.getTypeAt(elemFieldInd);
			return (TypeSerializer<C>) new ElementOrEventSerializer<C>(elemTpe.createSerializer(new ExecutionConfig()));
		}
	}

	public static final class ElementOrEventSerializer<T> extends TypeSerializer<ElementOrEvent<T>> {

		final TypeSerializer<T> elementSerializer;

		public ElementOrEventSerializer(TypeSerializer<T> elementSerializer) {
			this.elementSerializer = elementSerializer;
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return null;
		}

		@Override
		public CompatibilityResult<ElementOrEvent<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			return null;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<ElementOrEvent<T>> duplicate() {
			return this;
		}

		@Override
		public ElementOrEvent<T> createInstance() {
			return new ElementOrEvent<>();
		}

		@Override
		public ElementOrEvent<T> copy(ElementOrEvent<T> from) {
			return from.copy();
		}

		@Override
		public ElementOrEvent<T> copy(ElementOrEvent<T> from, ElementOrEvent<T> reuse) {
			return from.copy();
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(ElementOrEvent<T> r, DataOutputView target) throws IOException {
			target.writeByte(r.splitId);
			target.writeByte(r.logicalInputId);
			target.writeShort(r.subPartitionId);
			target.writeShort(r.targetPart);
			if (r.event != null) {
				serializeEvent(r, target);
			} else {
				assert r.element != null;
				target.writeBoolean(false); // mark that it's an Element
				elementSerializer.serialize(r.element, target);
			}
		}

		private void serializeEvent(ElementOrEvent<T> r, DataOutputView target) throws IOException {
			target.writeBoolean(true); // mark that it's an Event
			target.writeInt(r.event.type.ordinal());
			target.writeShort(r.event.assumedTargetPara);
			target.writeInt(r.event.bagID.cflSize);
			target.writeInt(r.event.bagID.opID);
		}

		@Override
		public ElementOrEvent<T> deserialize(DataInputView source) throws IOException {
			ElementOrEvent<T> r = new ElementOrEvent<T>();
			deserialize(r, source);
			return r;
		}

		@Override
		public ElementOrEvent<T> deserialize(ElementOrEvent<T> r, DataInputView s) throws IOException {
			r.splitId = s.readByte();
			r.logicalInputId = s.readByte();
			r.subPartitionId = s.readShort();
			r.targetPart = s.readShort();
			boolean isEvent = s.readBoolean();
			if (isEvent) {
				deserializeEvent(r, s);
			} else {
				r.element = elementSerializer.deserialize(s);
			}
			return r;
		}

		private void deserializeEvent(ElementOrEvent<T> r, DataInputView s) throws IOException {
			r.event = new Event();
			r.event.type = Event.enumConsts[s.readInt()];
			r.event.assumedTargetPara = s.readShort();
			r.event.bagID = new BagID();
			r.event.bagID.cflSize = s.readInt();
			r.event.bagID.opID = s.readInt();
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			ElementOrEvent<T> ee = deserialize(source);
			serialize(ee, target);
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof ElementOrEventSerializer && elementSerializer.equals(((ElementOrEventSerializer) obj).elementSerializer);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof ElementOrEventSerializer;
		}

		@Override
		public int hashCode() {
			return 43;
		}
	}
}
