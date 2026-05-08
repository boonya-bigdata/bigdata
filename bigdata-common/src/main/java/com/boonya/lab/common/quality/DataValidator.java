package com.boonya.lab.common.quality;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * 数据质量校验器 — 对流入数据管道的每条记录进行质量校验
 * 脏数据通过 side-output 输出，不阻塞主流程
 *
 * @param <T> 待校验的数据类型
 */
public class DataValidator<T> {

    private final List<Rule<T>> rules = new ArrayList<>();

    public DataValidator<T> addRule(String name, Predicate<T> predicate, String errorMsg) {
        rules.add(new Rule<>(name, predicate, errorMsg));
        return this;
    }

    public ValidationResult validate(T data) {
        List<String> errors = new ArrayList<>();
        for (Rule<T> rule : rules) {
            if (!rule.predicate.test(data)) {
                errors.add(rule.name + ": " + rule.errorMsg);
            }
        }
        return new ValidationResult(errors.isEmpty(), errors, data);
    }

    public record ValidationResult(boolean valid, List<String> errors, Object data) {
        public boolean isInvalid() { return !valid; }
    }

    private record Rule<T>(String name, Predicate<T> predicate, String errorMsg) {}
}
