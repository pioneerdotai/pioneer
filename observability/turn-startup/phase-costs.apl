['pioneer-traces']
| where ['resource.deployment.environment.name'] == 'production'
| where ['service.name'] == 'pioneer-gateway'
| extend phase_ms=toreal(['attributes.custom']['stage.duration_ms']),
    runtime=tostring(['attributes.custom']['runtime.kind']),
    input=tostring(['attributes.custom']['input.kind']),
    completion=tostring(['attributes.custom']['stage.completion']),
    db_admission_ms=toreal(['attributes.custom']['stage.db.admission_ms']),
    db_pool_ms=toreal(['attributes.custom']['stage.db.pool_ms']),
    db_execute_ms=toreal(['attributes.custom']['stage.db.execute_ms']),
    db_commit_ms=toreal(['attributes.custom']['stage.db.commit_ms']),
    db_execute_count=tolong(['attributes.custom']['stage.db.execute.count']),
    quanta=tolong(['attributes.custom']['stage.work.quanta']),
    messages=tolong(['attributes.custom']['stage.work.messages'])
| where isnotnull(phase_ms)
| summarize observations=count(), avg_ms=avg(phase_ms), p95_ms=percentile(phase_ms, 95),
    avg_db_admission_ms=avg(db_admission_ms), avg_db_pool_ms=avg(db_pool_ms),
    avg_db_execute_ms=avg(db_execute_ms), avg_db_commit_ms=avg(db_commit_ms),
    avg_db_execute_count=avg(db_execute_count), avg_quanta=avg(quanta), avg_messages=avg(messages)
    by name, ['service.version'], runtime, input, completion
| sort by avg_ms desc
