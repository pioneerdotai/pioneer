# Gateway CLI MCP bridge fixtures

The Gateway full-path harness uses deterministic in-memory provider and
invoker fixtures. Secret-bearing bootstrap material is generated only inside
owner-scoped temporary directories and is consumed during each test.
