"""Render the dbt model DAG (from target/manifest.json) as a Mermaid diagram in docs/dbt-dag.md."""
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
m = json.loads((root / "dbt/target/manifest.json").read_text())
nodes = {**m["nodes"], **m["sources"]}
label = lambda uid: nodes[uid]["name"] if uid.startswith("model.") else f'{nodes[uid]["source_name"]}.{nodes[uid]["name"]}'
ident = lambda uid: uid.replace(".", "_").replace("-", "_")
mat = {uid: n["config"].get("materialized", "") for uid, n in m["nodes"].items() if uid.startswith("model.")}
lines = ["# dbt model DAG", "", f"Generated from `dbt/target/manifest.json` ({sum(1 for u in mat)} models, "
         f"{sum(1 for u in m['sources'])} sources). Do not edit by hand: `python scripts/dbt_dag.py`.", "", "```mermaid", "flowchart LR"]
used = set()
edges = []
for uid in mat:
    for p in m["parent_map"].get(uid, []):
        if p.startswith(("model.", "source.")):
            edges.append((p, uid)); used |= {p, uid}
for uid in sorted(used):
    tag = f' ({mat[uid]})' if uid in mat else ""
    shape = ("[({0})]" if uid.startswith("source.") else "[{0}]").format(f'"{label(uid)}{tag}"')
    lines.append(f"  {ident(uid)}{shape}")
lines += [f"  {ident(a)} --> {ident(b)}" for a, b in sorted(edges)]
lines += ["```", ""]
(root / "docs/dbt-dag.md").write_text("\n".join(lines))
print("wrote docs/dbt-dag.md", len(edges), "edges", file=sys.stderr)
