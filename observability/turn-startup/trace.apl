['pioneer-traces']
| where trace_id == 'REPLACE_WITH_TRACE_ID'
| sort by _time asc
| project _time, name, duration, span_id, parent_span_id, ['service.name'], ['attributes.custom']
