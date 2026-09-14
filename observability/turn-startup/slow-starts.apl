['pioneer-traces']
| where ['resource.deployment.environment.name'] == 'production'
| where name == 'client.turn.startup' or name == 'gateway.turn.startup' or name == 'client.first_output.observe'
| extend startup_ms=toreal(['attributes.custom']['startup.duration_ms']),
    platform=tostring(['attributes.custom']['client.platform']),
    runtime=tostring(['attributes.custom']['runtime.kind']),
    input=tostring(['attributes.custom']['input.kind']),
    launch_path=tostring(['attributes.custom']['launch.path']),
    thread_role=tostring(['attributes.custom']['thread.role']),
    outcome=tostring(['attributes.custom']['outcome']),
    unattributed_ms=toreal(['attributes.custom']['startup.unattributed_ms'])
| top 100 by startup_ms desc
| project _time, trace_id, name, ['service.name'], ['service.version'], platform, runtime, input, launch_path, thread_role, outcome, startup_ms, unattributed_ms
