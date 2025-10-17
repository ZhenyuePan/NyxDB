#!/usr/bin/env python3
"""Generate sample multi-node NyxDB server configs under ./configs.

Each node gets a unique engine dir, cluster/grpc/metrics addresses.
The script accepts optional --nodes (count) starting IDs from 1 and
writes configs/node<N>.yaml.
"""

import argparse
import pathlib
import textwrap


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate multi-node configs")
    parser.add_argument("--nodes", type=int, default=3, help="number of nodes to generate (default: 3)")
    parser.add_argument("--base-dir", default="configs", help="output directory for yaml files")
    parser.add_argument("--engine-dir", default="/tmp/nyxdb-node{n}", help="engine dir template")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    template = textwrap.dedent(
        """\
        nodeID: {id}
        engine:
          dir: {engine_dir}
          enableDiagnostics: true
        cluster:
          clusterMode: true
          nodeAddress: 127.0.0.1:1900{id}
          clusterAddresses:
{cluster_lines}
          autoSnapshot: true
          snapshotInterval: 5m
          snapshotThreshold: 2048
          snapshotCatchUpEntries: 128
          snapshotDirSizeThreshold: 0
          snapshotMaxDuration: 2m
          snapshotMinInterval: 2m
          snapshotMaxAppliedLag: 0
        grpc:
          address: 127.0.0.1:1000{id}
        observability:
          metricsAddress: 127.0.0.1:211{id}
        """
    )

    cluster_addresses = [f"    - {idx}@127.0.0.1:1900{idx}" for idx in range(1, args.nodes + 1)]
    cluster_block = "\n".join(cluster_addresses)

    for node_id in range(1, args.nodes + 1):
        engine_path = args.engine_dir.format(n=node_id, id=node_id)
        content = template.format(
            id=node_id,
            engine_dir=engine_path,
            cluster_lines=cluster_block,
        )
        target = out_dir / f"node{node_id}.yaml"
        target.write_text(content, encoding="utf-8")
        print(f"generated {target}")


if __name__ == "__main__":
    main()
