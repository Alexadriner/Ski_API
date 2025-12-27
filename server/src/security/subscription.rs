#[derive(Debug, Clone)]
pub struct RateLimit {
    pub per_minute: Option<u32>,
    pub per_month: Option<u32>,
}

pub fn get_limits(plan: &str) -> RateLimit {
    match plan {
        "Free" => RateLimit {
            per_minute: Some(60),
            per_month: Some(2_500),
        },
        "Starter" => RateLimit {
            per_minute: Some(300),
            per_month: Some(100_000),
        },
        "Pro" => RateLimit {
            per_minute: Some(1_000),
            per_month: Some(500_000),
        },
        "Business" => RateLimit {
            per_minute: Some(3_000),
            per_month: Some(3_000_000),
        },
        "Enterprise" => RateLimit {
            per_minute: None,
            per_month: None,
        },
        _ => RateLimit {
            per_minute: Some(60),
            per_month: Some(2_500),
        },
    }
}