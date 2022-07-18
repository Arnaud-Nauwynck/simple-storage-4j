package org.simplestorage4j.api;

import java.util.Objects;

public class BlobStorageId {

	public final String id;

	// ------------------------------------------------------------------------
	
	public BlobStorageId(String id) {
		this.id = id;
	}

	public static BlobStorageId of(String id) {
		return new BlobStorageId(id);
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
		BlobStorageId other = (BlobStorageId) obj;
		return Objects.equals(id, other.id);
	}

}
