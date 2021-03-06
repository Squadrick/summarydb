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

struct DataTable {
    count @0 :Float64;
    max @1 :Float64;
    sum @2 :Float64;
}

struct ProtoSummaryWindow {
    ts @0 :Int64;
    te @1 :Int64;
    cs @2 :Int64;
    ce @3 :Int64;
    opData @4 :DataTable;
}

struct ProtoLandmarkWindow {
    ts @0 :Int64;
    te @1 :Int64;
    timestamps @2 :List(Int64);
    values @3 :List(Float64);
}

struct ExpWindow {
    base @0 :Float64;
}

struct PowerWindow {
    p @0 :Int64;
    q @1 :Int64;
    r @2 :Int64;
    s @3 :Int64;
}

struct Stream {
    id @0 :Int64;
    operators @1 :List(OpType);
    window :union {
        exp @2 :ExpWindow;
        power @3 :PowerWindow;
    }
}

struct DB {
    streamIds @0 :List(Int64);
}

struct HeapItem {
    value @0 :Int64;
    priority @1 :Int32;
    index @2 :Int32;
}

struct Heap {
    items @0 :List(HeapItem);
}

struct MergerIndexItem {
    swid @0 :Int64;
    cEnd @1 :Int64;
}

struct MergerIndex {
    items @0 :List(MergerIndexItem);
}
