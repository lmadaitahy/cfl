package gg.util;

import gg.ElementOrEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class Util {

	public static <T> TypeInformation<ElementOrEvent<T>> tpe() {
		return TypeInformation.of((Class<ElementOrEvent<T>>)(Class)ElementOrEvent.class);
	}
}
