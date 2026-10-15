`pioneer-metrics`:`pioneer.turn.startup.stage.duration`
| where `deployment.environment.name` == "production"
| bucket by `launch.path`, `thread.role`, `service.name`, `stage`, `runtime.kind`, `input.kind` to 5m using interpolate_delta_histogram(count, 0.5, 0.95, 0.99)
