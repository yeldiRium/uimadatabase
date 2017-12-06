package org.hucompute.services.uima.database.mongo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class Utils {

	public static <E> ArrayList<E> list(E... elements) {
		ArrayList<E> list = new ArrayList<E>(elements.length);
		Collections.addAll(list, elements);
		return list;
	}

	public static <E> HashSet<E> set(E... elements) {
		HashSet<E> set = new HashSet<E>(elements.length);
		Collections.addAll(set, elements);
		return set;
	}
	
	public static <K, V> HashMap<K, V> map() {
        return new HashMap<K, V>();
    }

	public static <T> T checkNotNull(T reference, String errorMessage) {
		if (reference == null) {
			throw new NullPointerException(errorMessage);
		}
		return reference;
	}

	public static void checkArgument(boolean expression, String errorMessage) {
		if (!expression) {
			throw new IllegalArgumentException(errorMessage);
		}
	}

}
