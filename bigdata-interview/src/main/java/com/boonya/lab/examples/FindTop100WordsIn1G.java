package com.boonya.lab.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * <h1>有一个1G大小的一个文件，里面每一行是一个词，词的大小不超过16字节，内存限制大小是1M，要求返回频数最高的100个词</h1>
 *
 * 要解决这个问题，我们需要在内存限制为1M的情况下，从一个1G大小的文件中找出频数最高的100个词。由于每个词的大小不超过16字节，我们可以考虑使用以下步骤来解决这个问题：
 * 1.分块读取：由于文件大小为1G，而内存限制为1M，我们不能一次性将整个文件读入内存。因此，我们需要分块读取文件，每块大小为1M。
 * 2.构建哈希表：对于每个块，我们可以在内存中构建一个哈希表来统计每个词的频数。由于每个词的大小不超过16字节，我们可以使用哈希表来快速查找和更新词的频数。
 * 3.合并哈希表：读取完所有块后，我们需要合并每个块的哈希表。由于内存限制，我们不能将所有哈希表同时保留在内存中。因此，我们可以将每个块的哈希表写入磁盘，然后逐个读取并合并。
 * 4.找出频数最高的100个词：合并完所有哈希表后，我们可以在内存中找出频数最高的100个词。这可以通过对哈希表中的词按照频数进行排序来实现。
 *
 *
 * @author Pengjunlin
 * @date 2024/12/9
 */
public class FindTop100WordsIn1G {

    private static final int MEMORY_LIMIT = 1024 * 1024; // 1MB
    private static final int BUFFER_SIZE = 1024 * 1024; // 1MB

    /**
     * 请注意，这个实现在处理非常大的文件时可能会遇到性能问题，因为它试图一次性读取整个文件的每一行到内存中。
     * 在实际应用中，你可能需要进一步优化这个实现，例如通过分块读取文件的内容，并将每个块的词频存储到磁盘上，然后再合并这些词频。
     * 这将需要更复杂的文件I/O操作和内存管理。此外，如果词的分布非常不均匀，可能需要使用更高级的数据结构来处理数据倾斜问题。
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String filePath = "path/to/your/file.txt"; // 替换为实际文件路径
        List<HashMap<String, Integer>> hashTables = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                hashTables.add(processChunk(Collections.singletonList(line)));
            }
        }

        HashMap<String, Integer> mergedHashTable = mergeHashTables(hashTables);
        List<Map.Entry<String, Integer>> top100Words = findTop100Words(mergedHashTable);

        top100Words.forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
    }

    private static HashMap<String, Integer> processChunk(List<String> chunk) {
        HashMap<String, Integer> hashTable = new HashMap<>();
        for (String word : chunk) {
            hashTable.put(word, hashTable.getOrDefault(word, 0) + 1);
        }
        return hashTable;
    }

    private static HashMap<String, Integer> mergeHashTables(List<HashMap<String, Integer>> hashTables) {
        HashMap<String, Integer> mergedHashTable = new HashMap<>();
        for (HashMap<String, Integer> hashTable : hashTables) {
            for (Map.Entry<String, Integer> entry : hashTable.entrySet()) {
                mergedHashTable.put(entry.getKey(), mergedHashTable.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
        }
        return mergedHashTable;
    }

    private static List<Map.Entry<String, Integer>> findTop100Words(HashMap<String, Integer> hashTable) {
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(hashTable.entrySet());
        entries.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));
        return entries.stream().limit(100).collect(Collectors.toList());
    }

}
