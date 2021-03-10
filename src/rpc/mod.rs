/// a
pub mod kvs_service {
    mod include {
        tonic::include_proto!("kvs");
    }
    pub use include::kv_rpc_client::KvRpcClient;
    pub use include::kv_rpc_server::{KvRpc, KvRpcServer};
    pub use include::{GetReply, GetRequest, RemoveReply, RemoveRequest, SetReply, SetRequest};
}
