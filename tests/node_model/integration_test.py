"""Functional tests through the real analytical frontend and Chakra ET reader."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from chakra.schema.protobuf.et_def_pb2 import (
    AttributeProto, GlobalMetadata, Node, COMP_NODE,
    COMM_COLL_NODE, COMM_SEND_NODE, COMM_RECV_NODE, METADATA_NODE, MEM_LOAD_NODE,
)
from chakra.src.third_party.utils.protolib import encodeMessage

parser = argparse.ArgumentParser()
parser.add_argument("--binary", required=True, type=Path)
parser.add_argument("--repo", required=True, type=Path)
options, remaining = parser.parse_known_args()
REPO = options.repo.resolve()
BINARY = options.binary.resolve()


def ref(buffer_id, size, storage="shared0"):
    return dict(storage=storage, buffer_id=buffer_id, generation=0, offset=0, bytes=size)


class Integration(unittest.TestCase):
    def run_model(self, kind="compute", *, slow=False, groups=False, singleton=False, physical_2d=False, small_input=False, external_slow=False, remote_mode="PER_NPU_MEMORY_EXPANSION", failure=None, mutate=None):
        config = json.loads((REPO / "examples/node/mesh-node.json").read_text())
        config["operation_profiles"] = "profiles.json"
        if slow:
            config["noc"]["link_bytes_per_ns"] = 2
        profiles = json.loads((REPO / "examples/node/mesh-node-profiles.json").read_text())
        profiles["profiles"]["pure"] = dict(kind="compute", cu_kind="tensor", stages=[
            dict(id="compute", kind="compute", ops=10240, deps=[])])
        system = json.loads((REPO / "examples/system/native_collectives/Ring_4chunks.json").read_text())
        system["node-model-config"] = "node.json"
        if physical_2d:
            for key in ["all-reduce-implementation", "all-gather-implementation", "reduce-scatter-implementation", "all-to-all-implementation"]:
                system[key] = ["ring", "ring"]
        members = 1 if singleton else 2 if groups else 4
        collective_codes = dict(all_reduce=0, all_gather=2, reduce_scatter=7, all_to_all=6)
        operation = "all_reduce" if kind == "mixed" else kind
        is_collective = operation in collective_codes
        input_size = 8 if small_input else 256 if kind == "all_gather" else 1024
        output_size = input_size * members if kind == "all_gather" else input_size // members if kind == "reduce_scatter" else input_size
        if is_collective or kind in {"point", "unmatched"}:
            for name, size, initialized in [("cin", input_size, True), ("cout", output_size, False), ("workspace", input_size, False)]:
                profiles["initial_buffers"].append(dict(storage="shared0", buffer_id=name, generation=0, bytes=size, initialized=initialized))
        with tempfile.TemporaryDirectory(prefix="astra-node-integration-") as directory:
            root = Path(directory)
            for rank in range(4):
                profile_name = "tile_op" if kind == "memory" else "pure"
                nodes = []
                if is_collective:
                    profile_name = f"collective-{rank}"
                    participants = [rank] if singleton else [rank // 2 * 2, rank // 2 * 2 + 1] if groups else list(range(4))
                    profiles["profiles"][profile_name] = dict(kind="collective", operation=operation,
                        input=ref("cin", input_size), output=ref("cout", output_size),
                        workspace=ref("workspace", input_size) if kind in {"reduce_scatter", "all_to_all"} else ref("cout", output_size),
                        participants=participants, instance_id=0, elem_bytes=4)
                    node = Node(id=1, name=operation, type=COMM_COLL_NODE)
                    node.attr.extend([AttributeProto(name="comm_type", int64_val=collective_codes[operation]),
                                      AttributeProto(name="comm_size", int64_val=input_size)])
                    if groups or singleton:
                        node.attr.append(AttributeProto(name="pg_name", string_val=str(rank + 1 if singleton else rank // 2 + 1)))
                    profiles["profiles"]["consumer"] = dict(kind="compute", cu_kind="tensor", stages=[
                        dict(id="read", kind="read", buffer=ref("cout", output_size), deps=[])])
                    consumer = Node(id=2, name="consume-result", type=COMP_NODE, data_deps=[1])
                    nodes = [node, consumer]
                    if kind == "mixed":
                        node.id = 2
                        consumer.id = 4
                        consumer.data_deps[0] = 2
                        nodes = [Node(id=1, name="parallel-memory", type=COMP_NODE), node,
                                 Node(id=3, name="parallel-compute", type=COMP_NODE), consumer]
                elif kind in {"point", "unmatched"} and rank < (2 if kind == "point" else 1):
                    send = rank == 0 and kind == "point"
                    profile_name = f"point-{rank}"
                    profiles["profiles"][profile_name] = dict(kind="send" if send else "recv",
                        buffer=ref("cin" if send else "cout", 1024), instance_id=0)
                    node = Node(id=1, name=profile_name, type=COMM_SEND_NODE if send else COMM_RECV_NODE)
                    node.attr.extend([AttributeProto(name="comm_size", int64_val=1024),
                                      AttributeProto(name="comm_tag", int64_val=12),
                                      AttributeProto(name="comm_src", int64_val=0 if kind == "point" else 1),
                                      AttributeProto(name="comm_dst", int64_val=1 if send else 0)])
                    nodes = [node]
                elif kind == "remote":
                    profile_name = "remote-load"
                    target = ref("remote-target", 1024, "cu_mem0")
                    if rank == 0:
                        profiles["initial_buffers"].append(dict(storage="cu_mem0", buffer_id="remote-target", generation=0, bytes=1024, initialized=False))
                    profiles["profiles"][profile_name] = dict(kind="remote_memory", remote_bytes=1024, local_buffer=target, requester="cu0")
                    profiles["profiles"]["remote-consumer"] = dict(kind="compute", cu_kind="tensor", cu_id="cu0", stages=[dict(id="read", kind="read", buffer=target, deps=[])])
                    node = Node(id=1, name=profile_name, type=MEM_LOAD_NODE)
                    node.attr.append(AttributeProto(name="tensor_size", int64_val=1024))
                    nodes = [node, Node(id=2, name="remote-consumer", type=COMP_NODE, data_deps=[1])]
                elif kind == "empty":
                    nodes = []
                elif kind == "metadata":
                    nodes = [Node(id=1, name="metadata", type=METADATA_NODE)]
                elif kind == "compute":
                    nodes = [Node(id=i, name=f"compute-{i}", type=COMP_NODE, data_deps=deps)
                             for i, deps in [(1, []), (2, []), (3, [1, 2])]]
                else:
                    nodes = [Node(id=1, name=profile_name, type=COMP_NODE)]
                with (root / f"workload.{rank}.et").open("wb") as stream:
                    encodeMessage(stream, GlobalMetadata(version="0.0.4"))
                    for node in nodes:
                        node.attr.append(AttributeProto(name="is_cpu_op", bool_val=False))
                        selected = "remote-consumer" if node.name == "remote-consumer" else "consumer" if node.name == "consume-result" else "tile_op" if node.name == "parallel-memory" else "pure" if node.name == "parallel-compute" else profile_name
                        node.attr.append(AttributeProto(name="node_model_profile", string_val=selected))
                        encodeMessage(stream, node)
            if mutate:
                mutate(config, profiles, system)
            for name, value in [("node.json", config), ("profiles.json", profiles), ("system.json", system),
                                ("groups.json", {str(i + 1): [i] for i in range(4)} if singleton else {"1": [0, 1], "2": [2, 3]})]:
                (root / name).write_text(json.dumps(value))
            command = [str(BINARY), f"--workload-configuration={root / 'workload'}",
                       f"--system-configuration={root / 'system.json'}",
                       f"--network-configuration={REPO / 'examples/network/analytical/Ring_4npus.yml'}",
                       f"--remote-memory-configuration={REPO / 'examples/remote_memory/analytical/no_memory_expansion.json'}"]
            if physical_2d:
                (root / "network.yml").write_text("topology: [ Ring, Ring ]\nnpus_count: [ 2, 2 ]\nbandwidth: [ 50.0, 50.0 ]\nlatency: [ 500.0, 500.0 ]\n")
                command[3] = f"--network-configuration={root / 'network.yml'}"
            elif external_slow:
                (root / "network.yml").write_text("topology: [ Ring ]\nnpus_count: [ 4 ]\nbandwidth: [ 50.0 ]\nlatency: [ 2000.0 ]\n")
                command[3] = f"--network-configuration={root / 'network.yml'}"
            if kind == "remote":
                remote = {"memory-type": remote_mode, "remote-mem-latency": 100, "remote-mem-bw": 32,
                          "num-nodes": 1, "num-npus-per-node": 4}
                (root / "remote.json").write_text(json.dumps(remote))
                command[4] = f"--remote-memory-configuration={root / 'remote.json'}"
            if groups or singleton:
                command.append(f"--comm-group-configuration={root / 'groups.json'}")
            result = subprocess.run(command, cwd=root, text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=30)
            if failure:
                # The existing analytical backend exits(-1) for unsupported
                # external topologies; node model errors use main's exit 1.
                self.assertEqual(result.returncode, 255 if physical_2d else 1,
                                 result.stdout[-6000:])
                self.assertIn(failure, result.stdout)
                if failure != "NoProgress":
                    self.assertNotIn("NoProgress", result.stdout)
                return {}
            diagnostic_lines = []
            for line in result.stdout.splitlines():
                if "NodeModel:" not in line:
                    continue
                if "NoProgress" in line and " state=" in line:
                    prefix, state = line.split(" state=", 1)
                    parsed = json.loads(state)
                    diagnostic_lines.append(prefix + " " + json.dumps({key: parsed.get(key) for key in ["node_idle", "noc_idle", "workload_finished", "ready_nodes", "ongoing_nodes", "active_waits", "memory_active_requests", "memory_pending_initiator_requests"]}))
                else:
                    diagnostic_lines.append(line[:1200])
            diagnostics = "\n".join(diagnostic_lines)
            self.assertEqual(result.returncode, 0, diagnostics + "\n" + result.stdout[-3000:])
            summaries = [json.loads(line.split("node-model-summary ", 1)[1])
                         for line in result.stdout.splitlines() if "node-model-summary " in line]
            self.assertEqual(len(summaries), 4, result.stdout[-6000:])
            return {entry["rank"]: entry for entry in summaries}

    def test_two_cus_and_dependency(self):
        for stats in self.run_model().values():
            self.assertEqual(stats["operations"]["1"]["end_ns"], 12)
            self.assertEqual(stats["operations"]["2"]["end_ns"], 12)
            self.assertEqual(stats["operations"]["3"]["start_ns"], 12)
            self.assertEqual(stats["operations"]["3"]["end_ns"], 24)

    def test_explicit_memory_lifecycle(self):
        for stats in self.run_model("memory").values():
            private = stats["storages"]["cu_mem0"]
            self.assertEqual(private["peak_bytes"], 1024)
            self.assertEqual(private["free_count"], 1)
            self.assertEqual(private["occupied_or_reserved_bytes"], 0)
            self.assertEqual(sum(d["bytes"] for d in stats["dma_engines"].values()), 2048)

    def test_all_reduce(self):
        for stats in self.run_model("all_reduce").values():
            self.assertEqual(stats["network_endpoint"]["tx_bytes"], 1536)
            self.assertEqual(stats["network_endpoint"]["rx_bytes"], 1536)

    def test_reduce_scatter(self):
        for stats in self.run_model("reduce_scatter").values():
            self.assertEqual(stats["network_endpoint"]["tx_bytes"], 768)

    def test_all_gather(self):
        for stats in self.run_model("all_gather").values():
            self.assertEqual(stats["network_endpoint"]["tx_bytes"], 768)

    def test_all_to_all(self):
        for stats in self.run_model("all_to_all").values():
            # Existing Ring forwards messages for 1+2+3 hops, four streams of 64 B.
            self.assertEqual(stats["network_endpoint"]["tx_bytes"], 1536)

    def test_disjoint_communicators(self):
        for stats in self.run_model("all_reduce", groups=True).values():
            self.assertEqual(stats["network_endpoint"]["tx_bytes"], 1024)

    def test_local_contention_changes_external_events(self):
        fast = self.run_model("all_reduce")[0]
        slow = self.run_model("all_reduce", slow=True)[0]
        first = lambda stats: min(t["submitted_ns"] for t in stats["network_endpoint"]["transfers"] if t["direction"] == "send")
        self.assertGreater(first(slow), first(fast))
        self.assertGreater(slow["operations"]["1"]["end_ns"], fast["operations"]["1"]["end_ns"])

    def test_point_to_point(self):
        stats = self.run_model("point")
        self.assertEqual(stats[0]["network_endpoint"]["tx_bytes"], 1024)
        self.assertEqual(stats[1]["network_endpoint"]["rx_bytes"], 1024)

    def test_no_progress_is_failure(self):
        self.run_model("unmatched", failure="NoProgress")

    def test_roofline_conflict(self):
        self.run_model(failure="conflicts", mutate=lambda c, p, s: s.update({"roofline-enabled": 1}))

    def test_invalid_topology(self):
        def mutate(config, profiles, system):
            config["noc"]["attachments"][0]["router"] = [5, 0]
        self.run_model(failure="coordinate outside mesh", mutate=mutate)

    def test_empty_and_metadata_only(self):
        for kind in ["empty", "metadata"]:
            for stats in self.run_model(kind).values():
                self.assertEqual(stats["operations"], {})

    def test_separate_collective_workspaces(self):
        def mutate(config, profiles, system):
            for profile in profiles["profiles"].values():
                if profile["kind"] == "collective":
                    profile["workspace"] = ref("workspace", profile["output"]["bytes"])
                    for entry in profiles["initial_buffers"]:
                        if entry["buffer_id"] == "workspace":
                            entry["bytes"] = profile["output"]["bytes"]
        for kind in ["all_reduce", "all_gather"]:
            self.run_model(kind, mutate=mutate)

    def test_singleton_reduce_scatter(self):
        self.run_model("reduce_scatter", singleton=True)

    def test_singleton_output_as_workspace(self):
        def mutate(config, profiles, system):
            for profile in profiles["profiles"].values():
                if profile["kind"] == "collective":
                    profile["workspace"] = dict(profile["output"])
        self.run_model("reduce_scatter", singleton=True, mutate=mutate)

    def test_small_communicator_does_not_expand_data(self):
        def mutate(config, profiles, system):
            for profile in profiles["profiles"].values():
                if profile["kind"] == "collective":
                    profile["input"]["bytes"] = profile["output"]["bytes"] = profile["workspace"]["bytes"] = 8
                    profile["elem_bytes"] = 1
            # Keep ET comm_size consistent by rewriting the generated message below.
        # A dedicated input-size option preserves real ET/profile agreement.
        stats = self.run_model("all_reduce", groups=True, mutate=mutate, small_input=True)
        for entry in stats.values():
            self.assertEqual(entry["network_endpoint"]["tx_bytes"], 8)

    def test_all_to_all_workspace_alias_rejected(self):
        def mutate(config, profiles, system):
            for profile in profiles["profiles"].values():
                if profile["kind"] == "collective":
                    profile["workspace"] = dict(profile["input"])
        self.run_model("all_to_all", failure="workspace overlaps input", mutate=mutate)

    def test_mismatched_collective_instance(self):
        def mutate(config, profiles, system):
            profiles["profiles"]["collective-0"]["instance_id"] = 7
        self.run_model("all_reduce", failure="CollectiveMismatch", mutate=mutate)

    def test_backend_rejects_multidimensional_physical_network(self):
        self.run_model("all_reduce", physical_2d=True, failure="only support 1-dim topology")

    def test_compute_dma_collective_overlap(self):
        for entry in self.run_model("mixed").values():
            self.assertEqual(entry["operations"]["3"]["start_ns"], 0)
            self.assertEqual(entry["operations"]["3"]["end_ns"], 12)
            self.assertGreater(entry["operations"]["1"]["end_ns"], 12)
            self.assertEqual(entry["network_endpoint"]["tx_bytes"], 1536)

    def test_external_delay_changes_local_successor(self):
        fast = self.run_model("all_reduce")[0]
        slow = self.run_model("all_reduce", external_slow=True)[0]
        self.assertGreater(slow["operations"]["2"]["start_ns"], fast["operations"]["2"]["start_ns"])
        self.assertEqual(slow["operations"]["2"]["start_ns"], slow["operations"]["1"]["end_ns"])

    def test_remote_completion_and_private_storage(self):
        for mode in ["PER_NPU_MEMORY_EXPANSION", "PER_NODE_MEMORY_EXPANSION", "MEMORY_POOL"]:
            for entry in self.run_model("remote", remote_mode=mode).values():
                self.assertGreaterEqual(entry["operations"]["1"]["end_ns"], 132)
                self.assertEqual(entry["operations"]["2"]["start_ns"], entry["operations"]["1"]["end_ns"])

    def test_capacity_before_dma(self):
        def mutate(config, profiles, system):
            profiles["initial_buffers"].append(dict(storage="cu_mem0", buffer_id="resident", generation=0, bytes=65536, initialized=True))
        self.run_model("memory", failure="CapacityExceeded", mutate=mutate)

    def test_capacity_failure_after_timed_event(self):
        def mutate(config, profiles, system):
            profiles["initial_buffers"].append(dict(storage="cu_mem0", buffer_id="resident", generation=0, bytes=65536, initialized=True))
            stages = profiles["profiles"]["tile_op"]["stages"]
            stages.append(dict(id="delay", kind="compute", ops=10240, deps=[]))
            stages[0]["deps"] = ["delay"]
        self.run_model("memory", failure="CapacityExceeded", mutate=mutate)

    def test_trace_and_metrics(self):
        def mutate(config, profiles, system):
            config["trace"] = dict(enabled=True, max_events=300)
        for stats in self.run_model("memory", mutate=mutate).values():
            self.assertTrue(stats["trace"]["enabled"])
            self.assertEqual(len(stats["trace"]["events"]), 300)
            self.assertTrue(stats["trace"]["truncated"])
            self.assertTrue(any(e["event"] == "link_transmit" for e in stats["trace"]["events"]))
            self.assertTrue(all({"op", "txn", "parent", "stage", "rank", "module", "tick_ns"} <= e.keys() for e in stats["trace"]["events"]))
            self.assertGreater(stats["operations"]["1"]["memory_stall_ns"], 0)

    def test_many_storage_instances(self):
        def mutate(config, profiles, system):
            for i in range(20):
                entry = dict(config["storages"][2], id=f"local_extra_{i}", capacity_bytes=4096 + i * 64, bytes_per_ns=i + 1)
                config["storages"].append(entry)
        for stats in self.run_model(mutate=mutate).values():
            self.assertEqual(len(stats["storages"]), 26)

    def test_legacy_reference(self):
        with tempfile.TemporaryDirectory(prefix="astra-legacy-") as directory:
            root = Path(directory)
            generation = subprocess.run([sys.executable, str(REPO / "tests/rt_template/inputs/workload/gen_chakra_traces.py")], cwd=root, text=True, capture_output=True)
            self.assertEqual(generation.returncode, 0, generation.stderr)
            result = subprocess.run([str(BINARY), f"--workload-configuration={root / 'chakra_trace'}",
                f"--system-configuration={REPO / 'tests/rt_template/inputs/system_cfg.json'}",
                f"--network-configuration={REPO / 'tests/rt_template/inputs/network_cfg.yml'}",
                f"--remote-memory-configuration={REPO / 'tests/rt_template/inputs/remote_memory_cfg.json'}"],
                cwd=root, text=True, capture_output=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            import re
            clean = result.stdout
            for _ in range(3):
                clean = "\n".join(re.sub(r"\[[^]\n]*\] ", "", line, count=1) for line in clean.splitlines()) + "\n"
            self.assertEqual(clean, (REPO / "tests/rt_template/refs/stdout.txt").read_text())


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0], *remaining])
