package com.boonya.lab.common.quality;

/**
 * 用户行为事件数据校验规则 — 示例
 */
public final class UserEventValidator {

    private UserEventValidator() {}

    public static DataValidator<UserEvent> create() {
        return new DataValidator<UserEvent>()
            .addRule("userId", e -> e.userId() != null && e.userId() > 0, "userId 必须为正整数")
            .addRule("page",   e -> e.page() != null && !e.page().isBlank(), "page 不能为空")
            .addRule("amount", e -> e.amount() == null || e.amount() >= 0, "amount 不能为负数")
            .addRule("timestamp", e -> e.timestamp() != null && e.timestamp() > 0, "timestamp 无效");
    }

    public record UserEvent(Integer userId, String page, Double amount, Long timestamp) {}
}
