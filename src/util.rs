use jiff::{Timestamp, Unit};

/// Current time truncated to whole seconds, as used in Kubernetes timestamps.
pub fn now() -> Timestamp {
    Timestamp::now()
        .round(Unit::Second)
        .unwrap_or_else(|_| Timestamp::now())
}

pub fn rfc3339(ts: Timestamp) -> String {
    ts.to_string()
}

/// Seconds elapsed since an RFC 3339 timestamp, or `None` if absent or unparseable.
pub fn age_secs(timestamp: Option<&str>, now: Timestamp) -> Option<i64> {
    let then: Timestamp = timestamp?.parse().ok()?;
    Some(now.as_second() - then.as_second())
}

/// Whether a kube error is an HTTP 409 Conflict (optimistic concurrency failure).
pub fn is_conflict(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(status) if status.code == 409)
}

pub fn is_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(status) if status.code == 404)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age() {
        let now: Timestamp = "2026-09-16T12:00:30Z".parse().unwrap();
        assert_eq!(age_secs(Some("2026-09-16T12:00:00Z"), now), Some(30));
        assert_eq!(age_secs(Some("garbage"), now), None);
        assert_eq!(age_secs(None, now), None);
        assert!(!rfc3339(super::now()).contains('.'));
    }
}
