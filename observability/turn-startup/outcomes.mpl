`pioneer-metrics`:`pioneer.turn.startup.outcomes`
| where `deployment.environment.name` == "production"
| where `observation.scope` == "client"
| align to 5m using sum
| group by `launch.path`, `thread.role`, `client.platform`, `input.kind`, `runtime.kind`, `outcome` using sum
