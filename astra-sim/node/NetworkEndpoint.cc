#include "astra-sim/node/NetworkEndpoint.hh"

namespace AstraSim::NodeSim {
NetworkEndpoint::NetworkEndpoint(const NodeTopology& topology,
                                 NodeEventScheduler& clock,
                                 MemorySystem& memory,
                                 NodeStatistics& statistics)
    : topology_(topology),
      clock_(clock),
      memory_(memory),
      statistics_(statistics) {
    if (topology.configuration().contains("network_endpoint")) {
        config_ = topology.configuration()["network_endpoint"];
        id_ = config_["id"];
    }
}
void NetworkEndpoint::transport(NetworkCall send, NetworkCall recv) {
    send_ = std::move(send);
    recv_ = std::move(recv);
}
void NetworkEndpoint::retry() {
    if (retry_pending_) {
        return;
    }
    retry_pending_ = true;
    clock_.after(0, [this] {
        retry_pending_ = false;
        auto tx = std::move(tx_wait_), rx = std::move(rx_wait_);
        tx_wait_.clear();
        rx_wait_.clear();
        for (auto& job : tx) {
            job();
        }
        for (auto& job : rx) {
            job();
        }
    });
}
void NetworkEndpoint::send(
    BufferRef source, int peer, int tag, Done done, RequestContext context) {
    check(!id_.empty() && bool(send_), "network send transport unavailable");
    check(topology_.modules().at(source.storage)["scope"] == "node_shared",
          "send requires node_shared source");
    const auto limit = number(config_, "tx_buffer_bytes", id_);
    check(source.bytes <= limit,
          "CapacityExceeded NIC TX message exceeds staging capacity");
    memory_.retain(source, true);
    auto submit = std::make_shared<Done>();
    std::weak_ptr<Done> weak = submit;
    *submit = [this, source, peer, tag, done = std::move(done), weak, limit,
               context] {
        auto submit = weak.lock();
        check(bool(submit), "lost TX submission");
        if (source.bytes > limit - tx_used_) {
            tx_wait_.push_back([submit] { (*submit)(); });
            return;
        }
        tx_used_ += source.bytes;
        tx_peak_ = std::max(tx_peak_, tx_used_);
        memory_.access(
            id_, id_, source, false,
            [this, source, peer, tag, done, context] {
                const auto begin = std::max(clock_.now(), tx_port_);
                tx_port_ = add(
                    begin, add(number(config_, "startup_ns", id_, true),
                               service(source.bytes,
                                       rate(config_, "bytes_per_ns", id_))));
                clock_.after(tx_port_ - clock_.now(), [this, source, peer, tag,
                                                       done, context] {
                    const auto start = clock_.now();
                    transfers_.push_back({{"direction", "send"},
                                          {"peer", peer},
                                          {"tag", tag},
                                          {"bytes", source.bytes},
                                          {"submitted_ns", start}});
                    statistics_.record(context, "external_send", id_,
                                       {{"peer", peer},
                                        {"tag", tag},
                                        {"bytes", source.bytes}});
                    send_(peer, tag, source.bytes,
                          [this, source, done, start, context] {
                              clock_.after(
                                  0,
                                  [this, source, done, start, context] {
                                      network_wait_ = add(network_wait_,
                                                          clock_.now() - start);
                                      statistics_.add_time(
                                          context.operation,
                                          "external_network_wait_ns",
                                          clock_.now() - start);
                                      statistics_.record(
                                          context, "external_send_complete",
                                          id_, {{"peer_bytes", source.bytes}});
                                      tx_used_ -= source.bytes;
                                      tx_bytes_ = add(tx_bytes_, source.bytes);
                                      memory_.unretain(source);
                                      retry();
                                      done();
                                  },
                                  1);
                          });
                });
            },
            true, context);
    };
    clock_.after(0, [submit] { (*submit)(); });
}
void NetworkEndpoint::recv(BufferRef destination,
                           int peer,
                           int tag,
                           bool reduce,
                           Done done,
                           RequestContext context) {
    check(!id_.empty() && bool(recv_), "network recv transport unavailable");
    check(topology_.modules().at(destination.storage)["scope"] == "node_shared",
          "recv requires node_shared destination");
    const auto limit = number(config_, "rx_buffer_bytes", id_);
    check(destination.bytes <= limit,
          "CapacityExceeded NIC RX message exceeds staging capacity");
    memory_.retain(destination, reduce);
    auto submit = std::make_shared<Done>();
    std::weak_ptr<Done> weak = submit;
    *submit = [this, destination, peer, tag, reduce, done = std::move(done),
               weak, limit, context] {
        auto submit = weak.lock();
        check(bool(submit), "lost RX submission");
        if (destination.bytes > limit - rx_used_) {
            rx_wait_.push_back([submit] { (*submit)(); });
            return;
        }
        rx_used_ += destination.bytes;
        rx_peak_ = std::max(rx_peak_, rx_used_);
        const auto start = clock_.now();
        transfers_.push_back({{"direction", "recv"},
                              {"peer", peer},
                              {"tag", tag},
                              {"bytes", destination.bytes},
                              {"submitted_ns", start}});
        statistics_.record(
            context, "external_receive_posted", id_,
            {{"peer", peer}, {"tag", tag}, {"bytes", destination.bytes}});
        recv_(peer, tag, destination.bytes,
              [this, destination, reduce, done, start, context] {
                  clock_.after(
                      0,
                      [this, destination, reduce, done, start, context] {
                          network_wait_ =
                              add(network_wait_, clock_.now() - start);
                          statistics_.add_time(context.operation,
                                               "external_network_wait_ns",
                                               clock_.now() - start);
                          statistics_.record(context,
                                             "external_receive_arrived", id_,
                                             {{"bytes", destination.bytes}});
                          finish_receive(destination, reduce, done, context);
                      },
                      1);
              });
    };
    clock_.after(0, [submit] { (*submit)(); });
}
void NetworkEndpoint::finish_receive(BufferRef destination,
                                     bool reduce,
                                     Done done,
                                     RequestContext context) {
    const auto begin = std::max(clock_.now(), rx_port_);
    rx_port_ = add(begin, add(number(config_, "startup_ns", id_, true),
                              service(destination.bytes,
                                      rate(config_, "bytes_per_ns", id_))));
    auto finish = [this, destination, done] {
        memory_.unretain(destination);
        rx_used_ -= destination.bytes;
        rx_bytes_ = add(rx_bytes_, destination.bytes);
        retry();
        done();
    };
    clock_.after(rx_port_ - clock_.now(), [this, destination, reduce, finish,
                                           context] {
        if (!reduce) {
            memory_.access(id_, id_, destination, true, finish, true, context);
            return;
        }
        check(topology_.configuration().contains("collective_engine"),
              "reduction unit unavailable");
        const auto& engine = topology_.configuration()["collective_engine"];
        const auto unit = engine["id"].get<std::string>();
        memory_.payload(
            id_, unit, destination.bytes,
            [this, destination, finish, unit, context] {
                memory_.access(
                    unit, unit, destination, false,
                    [this, destination, finish, unit, context] {
                        const auto& engine =
                            topology_.configuration()["collective_engine"];
                        const auto start = std::max(clock_.now(), reduce_port_);
                        reduce_port_ = add(
                            start,
                            service(destination.bytes,
                                    rate(engine, "reduce_bytes_per_ns", unit)));
                        clock_.after(
                            reduce_port_ - clock_.now(),
                            [this, destination, finish, unit, context] {
                                memory_.access(unit, unit, destination, true,
                                               finish, true, context);
                            });
                    },
                    true, context);
            },
            context);
    });
}
bool NetworkEndpoint::idle() const {
    return tx_used_ == 0 && rx_used_ == 0 && tx_wait_.empty() &&
           rx_wait_.empty();
}
json NetworkEndpoint::statistics() const {
    return {{"tx_bytes", tx_bytes_},
            {"rx_bytes", rx_bytes_},
            {"tx_peak_bytes", tx_peak_},
            {"rx_peak_bytes", rx_peak_},
            {"external_wait_accumulated_ns", network_wait_},
            {"transfers", transfers_}};
}
}  // namespace AstraSim::NodeSim
