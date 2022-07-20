package org.simplestorage4j.api.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.val;

public final class BlobStorageUtils {

	private BlobStorageUtils() {
	}
	
	public static <TDest,TSrc> List<TDest> map(Collection<TSrc> src, Function<TSrc,TDest> mapFunc) {
		return src.stream().map(x -> mapFunc.apply(x)).collect(Collectors.toList());
	}

	public static <TDest,TSrc> List<TDest> mapIter(Iterable<TSrc> src, Function<TSrc,TDest> mapFunc) {
		val res = new ArrayList<TDest>(); 
		for(val x : src) {
			res.add(mapFunc.apply(x));
		}
		return res;
	}

}
