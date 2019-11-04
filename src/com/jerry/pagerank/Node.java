package com.jerry.pagerank;

import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;

public class Node {
	private double pageRank = 1.0;
	private String[] adjacentNodeNames;
	public static final char fieldSeparator = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}

	@Override
	public String toString() {
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append(pageRank);
		if (getAdjacentNodeNames() != null) {
			sBuilder.append("\t").append(StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
		}
		return sBuilder.toString();
	}

	// 1.0 A B
	public static Node formMr(String value) throws IOException {
		String[] words = StringUtils.splitPreserveAllTokens(value, fieldSeparator);
		if (words.length < 1)
			throw new IOException("Expected 1 or more parts but received " + words.length);
		Node node = new Node().setPageRank(Double.parseDouble(words[0]));
		if (words.length > 1) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(words, 1, words.length));
		}
		return node;
	}
	
	public static Node formMr(String pr,String value) throws IOException {
		return formMr(pr+fieldSeparator+value);
	}
	public boolean containsAdjacentNodes() {
		return adjacentNodeNames!=null&&adjacentNodeNames.length>0;
	}

}
