# Reports

Scenario JSON reports are generated here and ignored by version control.

Declarative reports include public response observations, current logical
version, consistency decisions, repair and resurrection attempts, retained
physical-content versions, collection runs, and invariant results. They
describe reference-model evidence only; `make validate-system` and the
reconciler smoke provide real-system conformance evidence.

Milestone 0.8 live evidence must additionally record the complete normalized
listing request, opaque-marker outcome, Ring version/hash, block operation and
block-list type, caller allow/deny result, Storage API version, canary cleanup
status, and hashes of signed staged/GC metadata. Release evidence is accepted
only when every created list/block canary has an explicit successful cleanup
observation.
