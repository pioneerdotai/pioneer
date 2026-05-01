use anyhow::{Context, bail};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use pioneer_protocol::{TaskTrigger, TaskTriggerSpec};
use std::str::FromStr;

use crate::TaskRuntimeResult;

#[derive(Debug, Clone, Copy, Default)]
pub struct TaskTriggerCalculator;

impl TaskTriggerCalculator {
    pub fn validate(spec: &TaskTriggerSpec) -> TaskRuntimeResult<()> {
        match spec {
            TaskTriggerSpec::Immediate => Ok(()),
            TaskTriggerSpec::ScheduledAt {
                scheduled_at,
                timezone,
            } => {
                if *scheduled_at <= 0 {
                    bail!("scheduled_at must be a positive unix timestamp");
                }
                if let Some(timezone) = timezone {
                    validate_timezone(timezone)?;
                }
                Ok(())
            }
            TaskTriggerSpec::Interval {
                interval_seconds,
                interval_anchor_at,
            } => {
                if *interval_seconds <= 0 {
                    bail!("interval_seconds must be positive");
                }
                if interval_anchor_at.is_some_and(|value| value <= 0) {
                    bail!("interval_anchor_at must be positive when present");
                }
                Ok(())
            }
            TaskTriggerSpec::Cron {
                cron_expr,
                timezone,
            } => {
                let tz = validate_timezone(timezone)?;
                CronSpec::parse(cron_expr)?;
                let _ = tz;
                Ok(())
            }
            TaskTriggerSpec::Manual { .. }
            | TaskTriggerSpec::External { .. }
            | TaskTriggerSpec::Dependency { .. } => Ok(()),
        }
    }

    pub fn initial_next_fire_at(
        spec: &TaskTriggerSpec,
        now: i64,
    ) -> TaskRuntimeResult<Option<i64>> {
        Self::validate(spec)?;
        match spec {
            TaskTriggerSpec::Immediate => Ok(Some(now)),
            TaskTriggerSpec::ScheduledAt { scheduled_at, .. } => Ok(Some(*scheduled_at)),
            TaskTriggerSpec::Interval {
                interval_seconds,
                interval_anchor_at,
            } => Ok(Some(next_interval_fire(
                interval_anchor_at.unwrap_or(now),
                *interval_seconds,
                now,
            ))),
            TaskTriggerSpec::Cron {
                cron_expr,
                timezone,
            } => next_cron_fire(cron_expr, timezone, now).map(Some),
            TaskTriggerSpec::Manual { .. }
            | TaskTriggerSpec::External { .. }
            | TaskTriggerSpec::Dependency { .. } => Ok(None),
        }
    }

    pub fn next_after_fire(trigger: &TaskTrigger, fired_at: i64) -> TaskRuntimeResult<Option<i64>> {
        match &trigger.spec {
            TaskTriggerSpec::Interval {
                interval_seconds,
                interval_anchor_at,
            } => Ok(Some(next_interval_fire(
                interval_anchor_at.unwrap_or(fired_at),
                *interval_seconds,
                fired_at,
            ))),
            TaskTriggerSpec::Cron {
                cron_expr,
                timezone,
            } => next_cron_fire(cron_expr, timezone, fired_at).map(Some),
            _ => Ok(None),
        }
    }
}

fn validate_timezone(timezone: &str) -> TaskRuntimeResult<Tz> {
    Tz::from_str(timezone).with_context(|| format!("invalid timezone `{timezone}`"))
}

fn next_interval_fire(anchor: i64, interval_seconds: i64, now: i64) -> i64 {
    if anchor > now {
        return anchor;
    }
    let elapsed = now.saturating_sub(anchor);
    let intervals = elapsed / interval_seconds + 1;
    anchor.saturating_add(intervals.saturating_mul(interval_seconds))
}

fn next_cron_fire(expr: &str, timezone: &str, now: i64) -> TaskRuntimeResult<i64> {
    let spec = CronSpec::parse(expr)?;
    let tz = validate_timezone(timezone)?;
    let mut candidate = now.saturating_add(60 - now.rem_euclid(60));
    let limit = now.saturating_add(366 * 24 * 60 * 60);
    while candidate <= limit {
        let utc = Utc
            .timestamp_opt(candidate, 0)
            .single()
            .context("failed to build UTC timestamp")?;
        let local = utc.with_timezone(&tz);
        if spec.matches(
            i64::from(local.minute()),
            i64::from(local.hour()),
            i64::from(local.day()),
            i64::from(local.month()),
            i64::from(local.weekday().num_days_from_sunday()),
        ) {
            return Ok(candidate);
        }
        candidate = candidate.saturating_add(60);
    }
    bail!("cron expression did not produce a fire time within one year")
}

#[derive(Debug, Clone)]
struct CronSpec {
    minute: CronField,
    hour: CronField,
    day: CronField,
    month: CronField,
    weekday: CronField,
}

impl CronSpec {
    fn parse(expr: &str) -> TaskRuntimeResult<Self> {
        let parts = expr.split_whitespace().collect::<Vec<_>>();
        if parts.len() != 5 {
            bail!("cron expression must have five fields");
        }
        Ok(Self {
            minute: CronField::parse(parts[0], 0, 59)?,
            hour: CronField::parse(parts[1], 0, 23)?,
            day: CronField::parse(parts[2], 1, 31)?,
            month: CronField::parse(parts[3], 1, 12)?,
            weekday: CronField::parse(parts[4], 0, 7)?,
        })
    }

    fn matches(&self, minute: i64, hour: i64, day: i64, month: i64, weekday: i64) -> bool {
        self.minute.matches(minute)
            && self.hour.matches(hour)
            && self.day.matches(day)
            && self.month.matches(month)
            && self.weekday.matches(if weekday == 0 { 7 } else { weekday })
    }
}

#[derive(Debug, Clone)]
struct CronField {
    values: Vec<i64>,
}

impl CronField {
    fn parse(input: &str, min: i64, max: i64) -> TaskRuntimeResult<Self> {
        let mut values = Vec::new();
        for part in input.split(',') {
            parse_cron_part(part, min, max, &mut values)?;
        }
        values.sort_unstable();
        values.dedup();
        if values.is_empty() {
            bail!("cron field `{input}` has no values");
        }
        Ok(Self { values })
    }

    fn matches(&self, value: i64) -> bool {
        self.values.binary_search(&value).is_ok()
            || (value == 0 && self.values.binary_search(&7).is_ok())
    }
}

fn parse_cron_part(part: &str, min: i64, max: i64, values: &mut Vec<i64>) -> TaskRuntimeResult<()> {
    let (range, step) = match part.split_once('/') {
        Some((range, step)) => {
            let step = step
                .parse::<i64>()
                .with_context(|| format!("invalid cron step `{step}`"))?;
            if step <= 0 {
                bail!("cron step must be positive");
            }
            (range, step)
        }
        None => (part, 1),
    };

    let (start, end) = if range == "*" {
        (min, max)
    } else if let Some((start, end)) = range.split_once('-') {
        (
            parse_cron_value(start, min, max)?,
            parse_cron_value(end, min, max)?,
        )
    } else {
        let value = parse_cron_value(range, min, max)?;
        (value, value)
    };
    if start > end {
        bail!("cron range start must not exceed end");
    }
    let mut value = start;
    while value <= end {
        values.push(if max == 7 && value == 0 { 7 } else { value });
        value = value.saturating_add(step);
    }
    Ok(())
}

fn parse_cron_value(value: &str, min: i64, max: i64) -> TaskRuntimeResult<i64> {
    let parsed = value
        .parse::<i64>()
        .with_context(|| format!("invalid cron value `{value}`"))?;
    if parsed < min || parsed > max {
        bail!("cron value `{parsed}` is outside {min}..={max}");
    }
    Ok(parsed)
}
