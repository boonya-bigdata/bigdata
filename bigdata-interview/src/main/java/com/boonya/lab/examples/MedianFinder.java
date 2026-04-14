package com.boonya.lab.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 在大数据集中寻找中位数，特别是当数据量超出内存限制时，需要采用特殊的算法和技术。中位数是将数据集分成两个相等部分的值，其中一个部分的所有值都比它小，另一个部分的所有值都比它大。对于大数据集，我们可以使用以下方法来寻找中位数：
 * <p>
 * 1. BFPRT算法（中位数的中位数算法）
 * BFPRT算法（中位数的中位数算法，也称为快速选择算法）是一种用于在大数据集中找到第k小元素的算法，它是基于快速排序的选择算法。要找中位数，我们可以将数据集中的数分成两部分，一部分包含一半的数，另一部分包含剩下的一半。中位数就是这两部分的分界线。
 * <p>
 * 2. 外部排序
 * 如果数据集太大，无法一次性读入内存，可以使用外部排序的方法：
 * 分块排序：将数据分成多个块，每个块可以加载到内存中进行排序，然后将每个有序的小块保存到临时文件中。
 * 合并：使用多路归并排序将所有有序的小块合并成一个最终的有序文件。
 * 找到中位数：根据数据集的大小，如果数据集的元素个数是奇数，则中位数是中间的元素；如果是偶数，则中位数是中间两个元素的平均值。
 * 这种方法需要大量的磁盘I/O操作，因此在实际应用中可能需要根据具体场景和数据特性进行调整和优化。
 *
 * @author Pengjunlin
 * @date 2024/12/9
 */
public class MedianFinder {

    public double findMedian(String filePath) throws IOException {
        // 读取文件并获取数据大小
        long totalNumbers = Files.lines(Paths.get(filePath)).count();
        long medianPosition = totalNumbers / 2;

        Stream<String> stream = Files.lines(Paths.get(filePath));

        Stream<Long> streamLong = stream.map(v -> {
            return Long.valueOf(v);
        });
        if (totalNumbers % 2 == 1) {
            return quickSelect(streamLong, 0, totalNumbers - 1, (int) (medianPosition + 1));
        } else {
            return 0.5 * (quickSelect(streamLong, 0, totalNumbers - 1, (int) medianPosition)
                    + quickSelect(streamLong, 0, totalNumbers - 1, (int) (medianPosition + 1)));
        }
    }

    private double quickSelect(java.util.stream.Stream<Long> numbers, long low, long high, int k) {
        Random rand = new Random();
        long pivot = numbers.skip(low + rand.nextInt((int) (high - low + 1))).findAny().get();
        long left = low, right = high;

        while (true) {
            while (numbers.skip(++left).filter(i -> i < pivot).findAny().isPresent()) ;
            while (numbers.skip(--right).filter(i -> i > pivot).findAny().isPresent()) ;

            if (left >= right) {
                return numbers.skip(left).findFirst().get();
            }

            long temp = numbers.skip(left).findFirst().get();
            long finalRight = right;
            long finalLeft = left;
            numbers.skip(left).findFirst().ifPresent(i -> numbers.skip(finalRight).findFirst().ifPresent(j -> {
                numbers.skip(finalLeft).findFirst().get();
                numbers.skip(finalRight).findFirst().get();
            }));
        }
    }

    public static void main(String[] args) {
        MedianFinder medianFinder = new MedianFinder();
        try {
            double median = medianFinder.findMedian("path/to/your/data.txt");
            System.out.println("The median is: " + median);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}