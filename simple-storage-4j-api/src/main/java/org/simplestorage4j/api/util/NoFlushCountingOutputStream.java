package org.simplestorage4j.api.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class NoFlushCountingOutputStream extends FilterOutputStream {

	private long count;

	public NoFlushCountingOutputStream(OutputStream out) {
	    super(out);
	  }

	public long getCount() {
		return count;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
		count += len;
	}

	@Override
	public void write(int b) throws IOException {
		out.write(b);
		count++;
	}

	@Override
	public void flush() throws IOException {
		// override to avoid propagation to underlying flush !
	}

	@Override
	public void close() throws IOException {
		out.close();
	}
}