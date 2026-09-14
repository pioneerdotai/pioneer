`pioneer-metrics`:`pioneer.turn.startup.first_output.duration`
| where `deployment.environment.name` == "production"
| where `receive_boundary` == "js_publication"
| bucket by `launch.path`, `thread.role`, `runtime.kind`, `input.kind`, `service.version` to 5m using interpolate_delta_histogram(count, 0.5, 0.95, 0.99)
