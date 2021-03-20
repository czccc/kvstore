pub mod kvs_service {
    mod include {
        tonic::include_proto!("kvs");
    }
    pub use include::kv_rpc_client::KvRpcClient;
    pub use include::kv_rpc_server::{KvRpc, KvRpcServer};
    pub use include::{
        CommitReply, CommitRequest, GetReply, GetRequest, PrewriteReply, PrewriteRequest, Snapshot,
        TsReply, TsRequest, WriteOp,
    };
}

pub mod raft_service {
    mod include {
        tonic::include_proto!("raft");
    }
    pub use include::raft_rpc_client::RaftRpcClient;
    pub use include::raft_rpc_server::{RaftRpc, RaftRpcServer};
    pub use include::{
        AppendEntriesArgs, AppendEntriesReply, HeartBeatArgs, HeartBeatReply, InstallSnapshotArgs,
        InstallSnapshotReply, LogEntry, RequestVoteArgs, RequestVoteReply,
    };

    impl std::fmt::Display for RequestVoteArgs {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "[RequestVoteArgs ID: {} Term: {} Log: {} {}]",
                self.candidate_id, self.term, self.last_log_index, self.last_log_term
            )
        }
    }

    impl std::fmt::Display for RequestVoteReply {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "[RequestVoteReply Term: {} Vote: {}]",
                self.term, self.vote_granted
            )
        }
    }

    impl std::fmt::Display for AppendEntriesArgs {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "[AppendEntriesArgs ID: {} Term: {} Commit: {} Log: {} {} {}]",
                self.leader_id,
                self.term,
                self.leader_commit,
                self.prev_log_index,
                self.prev_log_term,
                self.entries.len()
            )
        }
    }

    impl std::fmt::Display for AppendEntriesReply {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "[AppendEntriesReply Term: {} Success: {} Conflict: {} {}]",
                self.term, self.success, self.conflict_log_index, self.conflict_log_term
            )
        }
    }

    impl std::fmt::Display for InstallSnapshotArgs {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "[InstallSnapshotArgs ID: {} Term: {} Last: {} {} Data: {} {} Done: {}]",
                self.leader_id,
                self.term,
                self.last_included_index,
                self.last_included_term,
                self.offset,
                self.data.len(),
                self.done
            )
        }
    }

    impl std::fmt::Display for InstallSnapshotReply {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "[InstallSnapshotReply Term: {}]", self.term)
        }
    }
}
