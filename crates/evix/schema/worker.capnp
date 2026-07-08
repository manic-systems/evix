@0xa74c8d9fb2f2f947;

struct ClientMessage {
  union {
    setup @0 :Setup;
    work @1 :Work;
    shutdown @2 :Void;
  }
}

struct Setup {
  config @0 :WorkerConfig;
  token @1 :TextOpt;
  expectedStoreDir @2 :TextOpt;
  protocolVersion @3 :UInt32;
}

struct Work {
  attrPath @0 :List(Text);
}

struct ServerMessage {
  union {
    ready @0 :Void;
    event @1 :Event;
    status @2 :WorkerStatus;
    error @3 :Text;
  }
}

enum WorkerStatus {
  ready @0;
  restart @1;
}

struct WorkerConfig {
  input @0 :Input;
  autoArgs @1 :List(AutoArg);
  forceRecurse @2 :Bool;
  gcRootsDir @3 :TextOpt;
  maxMemorySize @4 :UInt64;
  meta @5 :Bool;
  showInputDrvs @6 :Bool;
  overrideInputs @7 :List(StringPair);
  nixOptions @8 :List(StringPair);
  itemTimeoutSeconds @9 :UInt64;
  lockedFlakeJson @10 :TextOpt;
}

struct Input {
  union {
    flake @0 :Text;
    expr @1 :Text;
    file @2 :Text;
  }
}

struct AutoArg {
  name @0 :Text;
  union {
    expr @1 :Text;
    str @2 :Text;
  }
}

struct StringPair {
  key @0 :Text;
  value @1 :Text;
}

struct TextOpt {
  union {
    none @0 :Void;
    some @1 :Text;
  }
}

struct Event {
  union {
    derivation @0 :Derivation;
    attrSet @1 :AttrSet;
    error @2 :EvalError;
  }
}

struct AttrSet {
  attr @0 :Text;
  attrPath @1 :List(Text);
  attrs @2 :List(Text);
}

struct Derivation {
  attr @0 :Text;
  attrPath @1 :List(Text);
  name @2 :Text;
  system @3 :Text;
  drvPath @4 :Text;
  outputs @5 :List(Output);
  metaJson @6 :TextOpt;
  inputDrvs @7 :List(InputDrv);
  constituents @8 :TextListOpt;
  gcRootError @9 :TextOpt;
}

struct Output {
  name @0 :Text;
  union {
    absent @1 :Void;
    path @2 :Text;
  }
}

struct InputDrv {
  drvPath @0 :Text;
  valueJson @1 :Text;
}

struct TextListOpt {
  union {
    none @0 :Void;
    some @1 :List(Text);
  }
}

struct EvalError {
  attr @0 :Text;
  attrPath @1 :List(Text);
  error @2 :Text;
  fatal @3 :Bool;
}
