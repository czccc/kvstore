pub mod kvs_service {
    mod include {
        tonic::include_proto!("kvs");
    }
    pub use include::kv_rpc_client::KvRpcClient;
    pub use include::kv_rpc_server::{KvRpc, KvRpcServer};
    pub use include::{GetReply, GetRequest, RemoveReply, RemoveRequest, SetReply, SetRequest};
}

pub mod raft_service {
    mod include {
        tonic::include_proto!("raft");
    }
    pub use include::raft_rpc_client::RaftRpcClient;
    pub use include::raft_rpc_server::{RaftRpc, RaftRpcServer};
    pub use include::{
        AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, LogEntry,
        RequestVoteArgs, RequestVoteReply,
    };
}
