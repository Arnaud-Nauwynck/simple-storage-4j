package org.simplestorage4j.s3;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.List;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ListObjectsAndCommonPrefixes {
    public List<S3ObjectSummary> objectSummmaries;
    public List<String> commonPrefixes;
}