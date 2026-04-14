package com.boonya.lab.examples;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * 外部排序
 *
 * 在处理大数据量的排序问题时，尤其是当数据量超出内存限制时，我们通常采用外部排序的方法。
 * 外部排序的基本思想是将大文件分割成多个小块，每个小块可以加载到内存中进行排序，
 * 然后将每个有序的小块保存到临时文件中，最后合并这些有序的小块以得到全局有序的结果。
 *
 * @author Pengjunlin
 * @date 2024/12/9
 */
public class ExternalSortExample {

    private static final String TEMP_DIR = "temp/";

    /**
     * 1. 分块排序： 首先，我们需要将大文件分割成多个小块，每个小块可以加载到内存中进行排序。
     *
     * 2. 合并排序结果
     * 在所有小块排序完成后，我们需要将这些有序的小块合并成一个最终的有序文件。上述代码中的mergeSortedFiles方法实现了这一功能，它使用优先队列（最小堆）来高效地合并多个有序文件。
     *
     * 3. 性能优化
     * 在处理大数据量的排序时，可以采取以下优化措施：
     *
     * 调整块大小：根据内存大小和性能需求调整分块的大小，以达到最佳的内存利用率和排序效率。
     * 使用多线程：在分块排序和合并排序过程中使用多线程，可以显著提高排序速度。
     * I/O优化：尽量减少磁盘I/O操作，可以使用缓存或内存映射文件来提高读取和写入效率。
     * 以上代码和解释提供了一个基本的框架，用于处理大数据量的外部排序问题。在实际应用中，可能需要根据具体场景和数据特性进行调整和优化。
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // 创建临时目录
        new File(TEMP_DIR).mkdirs();
        // 生成大数据文件
        generateLargeFile("data.txt", 1000000);
        // 分块排序
        List<File> sortedFiles = splitAndSortFile("data.txt", 100000);
        // 合并排序结果
        mergeSortedFiles(sortedFiles, "sorted_data.txt");
    }

    private static void generateLargeFile(String fileName, int size) throws IOException {
        Random random = new Random();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (int i = 0; i < size; i++) {
                writer.write(random.nextInt(size) + "\n");
            }
        }
    }

    private static List<File> splitAndSortFile(String fileName, int chunkSize) throws IOException {
        List<File> sortedFiles = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            List<Integer> chunk = new ArrayList<>();
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                chunk.add(Integer.parseInt(line));
                if (chunk.size() == chunkSize) {
                    sortedFiles.add(sortAndSaveChunk(chunk, count++));
                    chunk.clear();
                }
            }
            if (!chunk.isEmpty()) {
                sortedFiles.add(sortAndSaveChunk(chunk, count));
            }
        }
        return sortedFiles;
    }

    private static File sortAndSaveChunk(List<Integer> chunk, int count) throws IOException {
        Collections.sort(chunk);
        File sortedFile = new File(TEMP_DIR + "sorted_chunk_" + count + ".txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(sortedFile))) {
            for (Integer num : chunk) {
                writer.write(num + "\n");
            }
        }
        return sortedFile;
    }

    private static void mergeSortedFiles(List<File> sortedFiles, String outputFile) throws IOException {
        PriorityQueue<BufferedReader> pq = new PriorityQueue<>(Comparator.comparingInt(reader -> {
            try {
                return Integer.parseInt(reader.readLine());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        Map<BufferedReader, Integer> currentMap = new HashMap<>();
        for (File file : sortedFiles) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            currentMap.put(reader, Integer.parseInt(reader.readLine()));
            pq.add(reader);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            while (!pq.isEmpty()) {
                BufferedReader reader = pq.poll();
                int value = currentMap.get(reader);
                writer.write(value + "\n");
                String line = reader.readLine();
                if (line != null) {
                    currentMap.put(reader, Integer.parseInt(line));
                    pq.add(reader);
                } else {
                    reader.close();
                }
            }
        }
    }
}
