`pioneer-metrics`:`pioneer.turn.startup.runtime.first_output.duration`
| where `deployment.environment.name` == "production"
| bucket by `launch.path`, `thread.role`, `runtime.kind`, `input.kind`, `session.state`, `model.family`, `reasoning.effort` to 5m using interpolate_delta_histogram(count, 0.5, 0.95, 0.99)
