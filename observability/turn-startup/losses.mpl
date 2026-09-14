`pioneer-metrics`:`pioneer.turn.startup.observation.losses`
| where `deployment.environment.name` == "production"
| align to 5m using sum
| group by `service.name`, `reason` using sum
