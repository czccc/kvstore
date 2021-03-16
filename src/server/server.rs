use super::*;
use crate::Result;

pub(crate) enum ServerKind {
    Basic(KvsBasicServer),
    Raft(KvRaftServer),
}

/// A server that used to handle Rpc request from clients
pub struct KvsServer {
    server: ServerKind,
}

impl KvsServer {
    pub(crate) fn new(server: ServerKind) -> KvsServer {
        KvsServer { server }
    }
    /// return a builder to configure the server
    pub fn builder() -> KvsServerBuilder {
        KvsServerBuilder::default()
    }
    /// start RPC process
    pub fn start(self) -> Result<()> {
        match self.server {
            ServerKind::Basic(s) => s.start(),
            ServerKind::Raft(s) => s.start(),
        }
    }
}
