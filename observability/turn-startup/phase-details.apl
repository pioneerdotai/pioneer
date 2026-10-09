['pioneer-traces']
| where trace_id == 'REPLACE_WITH_TRACE_ID'
| where ['service.name'] == 'pioneer-gateway'
| extend phase_ms=toreal(['attributes.custom']['stage.duration_ms']),
    runtime=tostring(['attributes.custom']['runtime.kind']),
    completion=tostring(['attributes.custom']['stage.completion']),
    db_admission_count=tolong(['attributes.custom']['stage.db.admission.count']),
    db_admission_ms=toreal(['attributes.custom']['stage.db.admission_ms']),
    db_pool_count=tolong(['attributes.custom']['stage.db.pool.count']),
    db_pool_ms=toreal(['attributes.custom']['stage.db.pool_ms']),
    db_execute_count=tolong(['attributes.custom']['stage.db.execute.count']),
    db_execute_ms=toreal(['attributes.custom']['stage.db.execute_ms']),
    db_commit_count=tolong(['attributes.custom']['stage.db.commit.count']),
    db_commit_ms=toreal(['attributes.custom']['stage.db.commit_ms']),
    quanta=tolong(['attributes.custom']['stage.work.quanta']),
    pages=tolong(['attributes.custom']['stage.work.pages']),
    messages=tolong(['attributes.custom']['stage.work.messages']),
    branches=tolong(['attributes.custom']['stage.work.branches'])
| where isnotnull(phase_ms)
| sort by _time asc
| project _time, name, span_id, parent_span_id, phase_ms, runtime, completion,
    db_admission_count, db_admission_ms, db_pool_count, db_pool_ms,
    db_execute_count, db_execute_ms, db_commit_count, db_commit_ms,
    quanta, pages, messages, branches
