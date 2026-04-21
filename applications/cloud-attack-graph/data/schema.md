# Tiny fixture schema

## Vertex labels

- `Identity { uid, name, kind }`
- `Workload { uid, name, namespace, exposure }`
- `Secret { uid, name, kind }`
- `Resource { uid, name, kind, sensitivity }`

## Relationship types

- `RUNS_AS (Workload -> Identity)`
- `USES_SECRET (Workload -> Secret)`
- `CAN_ASSUME (Identity -> Identity)`
- `CAN_ACCESS (Identity -> Resource)`

## Intended semantics

- A workload can leak a secret.
- A workload runs as one identity.
- An identity may assume another identity.
- An identity may directly access one or more resources.

The blast-radius scenario starts from one `Secret` and asks which sensitive
`Resource` nodes become reachable after following the `RUNS_AS` and
`CAN_ASSUME` chain.
