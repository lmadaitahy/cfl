package gg;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;

public class InputSubpartition<T> {

	public enum Status {OPEN, CLOSED}

	ArrayList<ArrayList<T>> buffers;
	ArrayList<Integer> cflSizes; // ez egyutt mozog a buffers-zel

	Status status;

	boolean damming;

	public InputSubpartition() {
		buffers = new ArrayList<>();
		status = Status.CLOSED;
		cflSizes = new ArrayList<>();
		damming = false;
	}
}
