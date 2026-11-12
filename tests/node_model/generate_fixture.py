"""Generate real Chakra workloads and node configurations in an output directory."""
import json
from pathlib import Path
import sys

from chakra.schema.protobuf.et_def_pb2 import AttributeProto, GlobalMetadata, Node, COMP_NODE, COMM_COLL_NODE
from chakra.src.third_party.utils.protolib import encodeMessage


def generate(output: Path, repo: Path, scenario="compute"):
    output.mkdir(parents=True, exist_ok=True)
    config = json.loads((repo / "examples/node/mesh-node.json").read_text())
    config["operation_profiles"] = "profiles.json"
    profiles = json.loads((repo / "examples/node/mesh-node-profiles.json").read_text())
    profiles["profiles"]["pure"] = {
        "kind": "compute", "cu_kind": "tensor",
        "stages": [{"id": "compute", "kind": "compute", "ops": 10240, "deps": []}],
    }
    collective_codes = dict(all_reduce=0, all_gather=2, reduce_scatter=7, all_to_all=6)
    input_size = 256 if scenario == "all_gather" else 1024
    if scenario in collective_codes:
        output_size = input_size * 4 if scenario == "all_gather" else input_size // 4 if scenario == "reduce_scatter" else input_size
        for name, size, initialized in [("cin", input_size, True), ("cout", output_size, False), ("workspace", input_size, False)]:
            profiles["initial_buffers"].append(dict(storage="shared0", buffer_id=name, generation=0, bytes=size, initialized=initialized))
        ref = lambda name, size: dict(storage="shared0", buffer_id=name, generation=0, offset=0, bytes=size)
        profiles["profiles"]["collective"] = dict(kind="collective", operation=scenario, participants=list(range(4)),
            input=ref("cin", input_size), output=ref("cout", output_size), instance_id=0, elem_bytes=4)
        if scenario in {"reduce_scatter", "all_to_all"}:
            profiles["profiles"]["collective"]["workspace"] = ref("workspace", input_size)
    system = json.loads((repo / "examples/system/native_collectives/Ring_4chunks.json").read_text())
    system["node-model-config"] = "node.json"
    for name, value in [("node.json", config), ("profiles.json", profiles), ("system.json", system)]:
        (output / name).write_text(json.dumps(value, indent=2) + "\n")
    for rank in range(4):
        with (output / f"workload.{rank}.et").open("wb") as stream:
            encodeMessage(stream, GlobalMetadata(version="0.0.4"))
            for node_id, deps in ([(1, []), (2, []), (3, [1, 2])] if scenario == "compute" else [(1, [])]):
                node = Node(id=node_id, name=f"{scenario}-{node_id}", type=COMM_COLL_NODE if scenario in collective_codes else COMP_NODE, data_deps=deps)
                node.attr.append(AttributeProto(name="is_cpu_op", bool_val=False))
                node.attr.append(AttributeProto(name="node_model_profile", string_val="collective" if scenario in collective_codes else "tile_op" if scenario == "memory" else "pure"))
                if scenario in collective_codes:
                    node.attr.append(AttributeProto(name="comm_type", int64_val=collective_codes[scenario]))
                    node.attr.append(AttributeProto(name="comm_size", int64_val=input_size))
                encodeMessage(stream, node)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--scenario", default="compute", choices=["compute", "memory", "all_reduce", "reduce_scatter", "all_gather", "all_to_all"])
    args = parser.parse_args()
    generate(args.output.resolve(), Path(__file__).resolve().parents[2], args.scenario)
