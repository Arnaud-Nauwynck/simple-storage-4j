package org.simplestorage4j.api;

import java.util.Objects;

import javax.annotation.Nonnull;

public class BlobStorageGroupId {

	public final @Nonnull String id;

	// ------------------------------------------------------------------------
	
	public BlobStorageGroupId(@Nonnull String id) {
		this.id = Objects.requireNonNull(id);
	}

	public static BlobStorageGroupId of(@Nonnull String id) {
		return new BlobStorageGroupId(id);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return id;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BlobStorageGroupId other = (BlobStorageGroupId) obj;
		return Objects.equals(id, other.id);
	}

}
