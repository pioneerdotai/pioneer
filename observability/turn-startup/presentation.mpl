`pioneer-metrics`:`pioneer.turn.startup.first_presented.duration`
| where `deployment.environment.name` == "production"
| bucket by `launch.path`, `thread.role`, `client.platform`, `runtime.kind`, `input.kind`, `presentation.observation` to 5m using interpolate_delta_histogram(count, 0.5, 0.95, 0.99)
