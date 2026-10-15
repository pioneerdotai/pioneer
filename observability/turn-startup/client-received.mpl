`pioneer-metrics`:`pioneer.turn.startup.first_output.duration`
| where `deployment.environment.name` == "production"
| where `receive_boundary` == "rust_transport"
| bucket by `launch.path`, `thread.role`, `client.platform`, `runtime.kind`, `input.kind`, `service.version` to 5m using interpolate_delta_histogram(count, 0.5, 0.95, 0.99)
