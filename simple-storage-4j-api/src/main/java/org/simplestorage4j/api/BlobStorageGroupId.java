package org.simplestorage4j.api;

import java.util.Objects;

public class BlobStorageGroupId {

	public final String id;

	// ------------------------------------------------------------------------
	
	public BlobStorageGroupId(String id) {
		this.id = id;
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
