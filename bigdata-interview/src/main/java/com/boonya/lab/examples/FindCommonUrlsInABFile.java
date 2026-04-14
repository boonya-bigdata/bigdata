package com.boonya.lab.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * 给定a、b两个文件，各存放50亿个url，每个url各占64字节，内存限制是4G，让你找出a、b文件共同的url； 请用Java代码实现
 *
 *
 * 要在Java中实现找出两个文件中共有的URL，并且考虑到内存限制，我们可以使用分治策略，将每个文件分成多个块，然后对每个块进行处理。以下是一个可能的实现方案：
 * 1、分块处理：将每个文件分成多个块，每个块的大小根据内存限制来定。由于每个URL占64字节，4G内存可以处理大约6250万（4GB / 64字节）个URL。
 * 2、哈希处理：对于每个块，我们可以计算每个URL的哈希值，并将它们存储在一个HashSet中。
 * 3、查找共有URL：对于文件A的每个块，我们将其URL的哈希值存储在HashSet中。然后，对于文件B的每个块，我们检查每个URL是否在文件A的HashSet中出现过。如果出现过，那么这个URL就是共有的。
 * 4、合并结果：将所有共有的URL合并，并最终返回共有URL的数量或者将它们写入到一个新文件中。
 *
 * @author Pengjunlin
 * @date 2024/12/9
 */
public class FindCommonUrlsInABFile {

    private static final int MEMORY_LIMIT = 4 * 1024 * 1024 * 1024; // 4GB
    private static final int URL_SIZE = 64; // 64字节
    private static final int MAX_URLS_IN_MEMORY = MEMORY_LIMIT / URL_SIZE; // 最大URL数量

    /**
     * 请注意，这个代码示例是一个简化的版本，它没有考虑文件的分块读取和磁盘存储。
     * 在实际应用中，你可能需要实现一个更复杂的逻辑来处理文件的分块读取和存储，以及在内存中维护多个块的HashSet。
     * 此外，这个示例也没有考虑性能优化，例如使用并行流或分布式处理来加速查找过程。
     * 在处理如此大规模的数据时，可能需要使用更高级的技术，如MapReduce或Spark来实现。
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String fileAPath = "path/to/a.txt";
        String fileBPath = "path/to/b.txt";
        List<String> commonURLs = findCommonURLs(fileAPath, fileBPath);
        // 处理commonURLs，例如打印或写入到文件
    }

    private static List<String> findCommonURLs(String fileAPath, String fileBPath) throws IOException {
        List<String> commonURLs = new ArrayList<>();

        // 读取文件A的所有URL，并存储在HashSet中
        HashSet<String> urlsSetA = new HashSet<>();
        try (BufferedReader readerA = new BufferedReader(new FileReader(fileAPath))) {
            String url;
            while ((url = readerA.readLine()) != null) {
                urlsSetA.add(url);
                if (urlsSetA.size() >= MAX_URLS_IN_MEMORY) {
                    // 处理文件B的对应块
                    try (BufferedReader readerB = new BufferedReader(new FileReader(fileBPath))) {
                        String urlB;
                        while ((urlB = readerB.readLine()) != null && !urlsSetA.isEmpty()) {
                            if (urlsSetA.contains(urlB)) {
                                commonURLs.add(urlB);
                            }
                        }
                    }
                    // 重置HashSet以处理下一个块
                    urlsSetA.clear();
                }
            }
        }

        // 处理剩余的URL
        try (BufferedReader readerB = new BufferedReader(new FileReader(fileBPath))) {
            String urlB;
            while ((urlB = readerB.readLine()) != null) {
                if (urlsSetA.contains(urlB)) {
                    commonURLs.add(urlB);
                }
            }
        }

        return commonURLs;
    }

}
