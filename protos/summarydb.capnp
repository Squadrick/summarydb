using Go = import "/go.capnp";
@0x91f0805429cab961;
$Go.package("protos");
$Go.import("summarydb/protos");

enum OpType {
    count @0;
    sum @1;
    bloom @2;
    cms @3;
    max @4;
    freq @5;
}

struct Row {
    cell @0 :List(Int64);
}

struct ProtoCMS {
    size @0 :Int64;
    row @1 :List(Row);
}

struct ProtoOperator {
    operator :union {
        long @0 :Int64;
        bytearray @1 :Data;
        cms @2 :ProtoCMS;
    }
}

struct ProtoSummaryWindow {
    ts @0 :Int64;
    te @1 :Int64;
    cs @2 :Int64;
    ce @3 :Int64;
    operator @4 :List(ProtoOperator);
}

struct ProtoLandmarkWindow {
    ts @0 :Int64;
    te @1 :Int64;
    timestamp @2 :Int64;
    value @3 :Data;
}