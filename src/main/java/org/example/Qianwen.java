package org.example;

import java.util.List;
import java.util.ArrayList;

import com.alibaba.dashscope.embeddings.TextEmbedding;
import com.alibaba.dashscope.embeddings.TextEmbeddingParam;
import com.alibaba.dashscope.embeddings.TextEmbeddingResult;
import com.alibaba.dashscope.embeddings.TextEmbeddingResultItem;
import com.alibaba.dashscope.exception.ApiException;
import com.alibaba.dashscope.exception.NoApiKeyException;

public class Qianwen {
	public static List<List<Double>> batchEmbeddingText(List<String> inputs) {
		List<List<Double>> embeddings = new ArrayList<>();
		TextEmbeddingParam param = TextEmbeddingParam
				.builder()
				.model(TextEmbedding.Models.TEXT_EMBEDDING_V3)
				.texts(inputs).build();

		TextEmbedding textEmbedding = new TextEmbedding();
		TextEmbeddingResult result;

		try {
			result = textEmbedding.call(param);
			List<TextEmbeddingResultItem> ret = result.getOutput().getEmbeddings();
			for (TextEmbeddingResultItem item : ret) {
				embeddings.add(item.getEmbedding());
			}
		} catch (ApiException e) {
			e.printStackTrace();
		} catch (NoApiKeyException e) {
			e.printStackTrace();
		}
		
		return embeddings;
	}
}
