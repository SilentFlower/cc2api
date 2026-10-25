use std::collections::BTreeMap;

use once_cell::sync::Lazy;
use serde::Deserialize;

use crate::service::telemetry::MessageTelemetryUsage;

/// 从本机 sub2api 提取的版本化价格快照，不在业务请求中访问外部价格服务。
#[derive(Deserialize)]
struct PriceCatalog {
    version: String,
    prices: BTreeMap<String, Price>,
}

/// 单个模型的美元／Token 价格；字段沿用参考目录的名称。
#[derive(Deserialize)]
struct Price {
    input_cost_per_token: f64,
    output_cost_per_token: f64,
    cache_creation_input_token_cost: f64,
    cache_read_input_token_cost: f64,
    #[serde(default)]
    cache_creation_input_token_cost_above_1hr: f64,
    #[serde(default)]
    input_cost_per_token_priority: f64,
    #[serde(default)]
    output_cost_per_token_priority: f64,
    #[serde(default)]
    cache_creation_input_token_cost_priority: f64,
    #[serde(default)]
    cache_read_input_token_cost_priority: f64,
    #[serde(default)]
    fast_multiplier: Option<f64>,
    #[serde(default)]
    long_context_input_token_threshold: u64,
    #[serde(default)]
    long_context_input_cost_multiplier: f64,
    #[serde(default)]
    long_context_output_cost_multiplier: f64,
}

static CATALOG: Lazy<PriceCatalog> = Lazy::new(|| {
    serde_json::from_str(include_str!("claude-pricing.json")).expect("内置 Claude 价格快照必须有效")
});

/// 返回落库费用所采用的价格版本。
/// @return 内置价格快照的版本标识。
pub fn price_version() -> &'static str {
    &CATALOG.version
}

/// 按参考目录计算标准 API 折算费用；未知型号或模式不猜价。
/// @param model 上游实际模型名称。
/// @param speed 上游 usage 声明的 speed／service_tier。
/// @param usage 上游已观察的 Token 摘要。
/// @return 有依据的美元金额，无法定价时为 None。
pub fn calculate_cost(model: &str, speed: &str, usage: MessageTelemetryUsage) -> Option<f64> {
    let model = model.trim().to_ascii_lowercase().replace('.', "-");
    let model = model.strip_suffix("[1m]").unwrap_or(&model);
    let price = CATALOG.prices.get(model).or_else(|| {
        // 日期后缀只继承已知型号；不能把未知的下一代型号按同系列低价计算。
        model.rsplit_once('-').and_then(|(base, date)| {
            (date.len() == 8 && date.bytes().all(|c| c.is_ascii_digit()))
                .then(|| CATALOG.prices.get(base))
                .flatten()
        })
    })?;
    let priority = match speed {
        "" | "standard" | "default" => false,
        "fast" | "priority" => true,
        _ => return None,
    };
    let has_priority = price.input_cost_per_token_priority > 0.0
        || price.output_cost_per_token_priority > 0.0
        || price.cache_creation_input_token_cost_priority > 0.0
        || price.cache_read_input_token_cost_priority > 0.0;
    let use_priority = priority && has_priority && price.fast_multiplier.is_none();
    let tier_multiplier = if priority && !use_priority {
        price.fast_multiplier.unwrap_or(2.0)
    } else {
        1.0
    };
    let tier_price = |standard: f64, fast: f64| {
        if use_priority && fast > 0.0 {
            fast
        } else {
            standard
        }
    };
    let write_total = cache_write_tokens(usage);
    let context = usage
        .input_tokens
        .checked_add(usage.cache_read_input_tokens)?
        .checked_add(write_total)?;
    let long_context = price.long_context_input_token_threshold > 0
        && context > price.long_context_input_token_threshold;
    let input_multiplier = if long_context {
        price.long_context_input_cost_multiplier.max(1.0)
    } else {
        1.0
    };
    let output_multiplier = if long_context {
        price.long_context_output_cost_multiplier.max(1.0)
    } else {
        1.0
    };
    let mut write_5m = usage.cache_creation_ephemeral_5m_input_tokens;
    let mut write_1h = usage.cache_creation_ephemeral_1h_input_tokens;
    let details = write_5m.checked_add(write_1h)?;
    if usage.cache_creation_input_tokens > 0 && details > write_total {
        write_5m = ((write_total as f64 * write_5m as f64 / details as f64).round() as u64)
            .min(write_total);
        write_1h = write_total - write_5m;
    }
    let write_price = tier_price(
        price.cache_creation_input_token_cost,
        price.cache_creation_input_token_cost_priority,
    );
    let write_cost = if price.cache_creation_input_token_cost_above_1hr
        > price.cache_creation_input_token_cost
    {
        // 已知总量大于 TTL 明细时，未拆分的部分仍按默认 5m 计价，避免漏算。
        (write_total.saturating_sub(write_1h) as f64 * price.cache_creation_input_token_cost)
            + write_1h as f64 * price.cache_creation_input_token_cost_above_1hr
    } else {
        write_total as f64 * write_price
    };
    let cost = ((usage.input_tokens as f64
        * tier_price(
            price.input_cost_per_token,
            price.input_cost_per_token_priority,
        )
        + usage.cache_read_input_tokens as f64
            * tier_price(
                price.cache_read_input_token_cost,
                price.cache_read_input_token_cost_priority,
            )
        + write_cost)
        * input_multiplier
        + usage.output_tokens as f64
            * tier_price(
                price.output_cost_per_token,
                price.output_cost_per_token_priority,
            )
            * output_multiplier)
        * tier_multiplier;
    (cost.is_finite() && cost >= 0.0).then_some(cost)
}

/// 返回缓存写入总量，避免 TTL 分项与 aggregate 重复累计。
/// @param usage 上游用量摘要。
/// @return 已知 aggregate；缺少 aggregate 时使用两个 TTL 分项之和。
pub fn cache_write_tokens(usage: MessageTelemetryUsage) -> u64 {
    if usage.cache_creation_input_tokens > 0 {
        usage.cache_creation_input_tokens
    } else {
        usage
            .cache_creation_ephemeral_5m_input_tokens
            .saturating_add(usage.cache_creation_ephemeral_1h_input_tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_prices_separate_cache_ttls_and_versions() {
        let usage = MessageTelemetryUsage {
            input_tokens: 1_000_000,
            output_tokens: 100_000,
            cache_read_input_tokens: 2_000_000,
            cache_creation_input_tokens: 1_000_000,
            cache_creation_ephemeral_5m_input_tokens: 600_000,
            cache_creation_ephemeral_1h_input_tokens: 400_000,
        };
        let cost = calculate_cost("claude-fable-5-1[1m]", "standard", usage).unwrap();
        assert!((cost - 31.0).abs() < 1e-9);
        assert!((calculate_cost("claude-fable-5", "", usage).unwrap() - 32.5).abs() < 1e-9);
        assert!(calculate_cost("claude-fable-99", "", usage).is_none());
        assert!(calculate_cost("claude-fable-5", "unknown", usage).is_none());
        assert_eq!(cache_write_tokens(usage), 1_000_000);
    }

    #[test]
    fn fast_aliases_and_long_context_use_reference_policies() {
        let usage = MessageTelemetryUsage {
            input_tokens: 100_001,
            output_tokens: 10_000,
            ..Default::default()
        };
        let cost = calculate_cost("claude-haiku-5-5", "", usage).unwrap();
        assert!((cost - 0.0750005).abs() < 1e-9);
        let usage = MessageTelemetryUsage {
            input_tokens: 1_000_000,
            ..Default::default()
        };
        assert_eq!(calculate_cost("claude-opus-5-5", "fast", usage), Some(8.0));
        assert_eq!(
            calculate_cost("claude-opus-4.6-20260205", "", usage),
            Some(5.0)
        );
        assert_eq!(calculate_cost("claude-sonnet-5", "", usage), Some(3.0));
        let usage = MessageTelemetryUsage {
            input_tokens: 1_000_000,
            output_tokens: 100_000,
            ..Default::default()
        };
        assert!((calculate_cost("claude-sonnet-4-5", "", usage).unwrap() - 8.25).abs() < 1e-9);
    }
}
