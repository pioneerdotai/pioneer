`pioneer-metrics`:`pioneer.turn.startup.unattributed.duration`
| where `deployment.environment.name` == "production"
| bucket by `launch.path`, `thread.role`, `observation.scope`, `client.platform`, `runtime.kind`, `outcome` to 5m using interpolate_delta_histogram(count, 0.5, 0.95, 0.99)
