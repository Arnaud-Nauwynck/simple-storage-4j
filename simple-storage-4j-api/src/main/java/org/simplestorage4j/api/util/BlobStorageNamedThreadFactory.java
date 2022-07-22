package org.simplestorage4j.api.util;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.NonNull;
import lombok.val;

/**
 * default implementation of java ThreadFactory, using namePrefix+id+nameSuffix 
 */
public class BlobStorageNamedThreadFactory implements ThreadFactory {
	
	private final @NonNull String namePrefix;
	private final @NonNull String nameSuffix;
	private final boolean daemon;
	
	private final AtomicInteger threadIdGenerator = new AtomicInteger();

	// ------------------------------------------------------------------------
	
	public BlobStorageNamedThreadFactory(@NonNull String namePrefix, @NonNull String nameSuffix, boolean daemon) {
		this.namePrefix = Objects.requireNonNull(namePrefix);
		this.nameSuffix = Objects.requireNonNull(nameSuffix);
		this.daemon = daemon;
	}

	// ------------------------------------------------------------------------

	@Override
	public Thread newThread(Runnable r) {
		val id = threadIdGenerator.incrementAndGet();
		Thread res = new Thread(r, namePrefix + id + nameSuffix);
		if (daemon) {
			res.setDaemon(daemon);
		}
		return res;
	}

}
