package org.simplestorage4j.api.util;

import java.io.OutputStream;

public final class NullCountingOutputStream extends OutputStream {

	private long count;

	public void resetCount() {
		this.count = 0;
	}
	public long getCount() {
		return count;
	}

	@Override
	public void write(byte[] b, int off, int len) {
		count += len;
	}

	@Override
	public void write(int b) {
		count++;
	}

	@Override
	public void close() {
	}
}