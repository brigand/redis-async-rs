/*
 * Copyright 2017 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::{future, Future, Sink, Stream};
use futures::sync::{mpsc, oneshot};

use tokio_core::reactor::Handle;

use error;
use resp;
use super::connect::{connect, ClientConnection};

type PairedConnectionBox = Box<Future<Item = PairedConnection, Error = error::Error>>;

/// The default starting point to use most default Redis functionality.
///
/// Returns a future that resolves to a `PairedConnection`.
pub fn paired_connect(addr: &SocketAddr, handle: &Handle) -> PairedConnectionBox {
    let handle = handle.clone();
    let paired_con = connect(addr, &handle)
        .map(move |connection| {
            let ClientConnection { sender, receiver } = connection;
            let (out_tx, out_rx) = mpsc::unbounded();
            let sender = out_rx.fold(sender, |sender, msg| sender.send(msg).map_err(|_| ()));
            let resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>> =
                Arc::new(Mutex::new(VecDeque::new()));
            let receiver_queue = resp_queue.clone();
            let receiver = receiver.for_each(move |msg| {
                let mut queue = receiver_queue.lock().expect("Lock is tainted");
                let dest = queue.pop_front().expect("Queue is empty");
                match dest.send(msg) {
                    Ok(()) => Ok(()),
                    // Ignore error as the channel may have been legitimately closed in the meantime
                    Err(_) => Ok(())
                }
            });
            handle.spawn(sender.map(|_| ()));
            handle.spawn(receiver.map_err(|_| ()));
            PairedConnection {
                out_tx: out_tx,
                resp_queue: resp_queue,
            }
        })
        .map_err(|e| e.into());
    Box::new(paired_con)
}

pub struct PairedConnection {
    out_tx: mpsc::UnboundedSender<resp::RespValue>,
    resp_queue: Arc<Mutex<VecDeque<oneshot::Sender<resp::RespValue>>>>,
}

pub type SendBox<T> = Box<Future<Item = T, Error = error::Error>>;

/// Fire-and-forget, used to force the return type of a `send` command where the result is not required
/// to satisfy the generic return type.
///
#[macro_export]
macro_rules! faf {
    ($e:expr) => (
        {
            let _:$crate::client::paired::SendBox<$crate::resp::RespValue> = $e;
        }
    )
}

impl PairedConnection {
    /// Sends a command to Redis.
    ///
    /// The message must be in the format of a single RESP message, this can be constructed
    /// manually or with the `resp_array!` macro.  Returned is a future that resolves to the value
    /// returned from Redis.  The type must be one for which the `resp::FromResp` trait is defined.
    ///
    /// The future will fail for numerous reasons, including but not limited to: IO issues, conversion
    /// problems, and server-side errors being returned by Redis.
    ///
    /// Behind the scenes the message is queued up and sent to Redis asynchronously before the
    /// future is realised.  As such, it is guaranteed that messages are sent in the same order
    /// that `send` is called.
    pub fn send<T: resp::FromResp + 'static>(&self, msg: resp::RespValue) -> SendBox<T> {
        match &msg {
            &resp::RespValue::Array(_) => (),
            _ => {
                return Box::new(future::err(error::internal("Command must be a RespValue::Array")))
            }
        }

        let (tx, rx) = oneshot::channel();
        let mut queue = self.resp_queue.lock().expect("Tainted queue");

        queue.push_back(tx);

        self.out_tx.unbounded_send(msg).expect("Failed to send");

        let future = rx.then(|v| match v {
                                 Ok(v) => future::result(T::from_resp(v)),
                                 Err(e) => future::err(e.into()),
                             });
        Box::new(future)
    }
}

///
/// Implementing Redis commands as specific Rust functions, intended to be easier to use that manually constructing
/// each as appropriate.
///
/// Warning: this is still subject to change.  Only a subset of commands are implemented so far, and not done so
/// consistently.  This is ongoing to test various options, a winner will be picked in due course.
///
/// Protected by a feature flag until the above issues are satisfied.
///
mod commands {
    use std::mem;

    use futures::{future, Future};

    use error;
    use resp::{FromResp, ToRespString, RespValue};

    use super::SendBox;

    pub trait CommandAddable {
        fn add_to_cmd(self, &mut Vec<RespValue>);
        fn num_cmds() -> usize;
    }

    impl<T: ToRespString + Into<RespValue>> CommandAddable for (f64, f64, T) {
        fn add_to_cmd(self, cmd: &mut Vec<RespValue>) {
            let (lng, lat, dst) = self;
            cmd.push(lng.to_string().into());
            cmd.push(lat.to_string().into());
            cmd.push(dst.into());
        }

        fn num_cmds() -> usize {
            3
        }
    }

    impl<T: ToRespString + Into<RespValue>> CommandAddable for T {
        fn add_to_cmd(self, cmd: &mut Vec<RespValue>) {
            cmd.push(self.into());
        }

        fn num_cmds() -> usize {
            1
        }
    }

    /// Several Redis commands take an open-ended collection of keys, or other such structures that are flattened
    /// into the redis command.  For example `MGET key1 key2 key3`.
    ///
    /// The challenge for this library is anticipating how this might be used by applications.  It's conceivable that
    /// applications will use vectors, but also might have a fixed set of keys which could either be passed in an array
    /// or as a reference to a slice.
    ///
    pub trait CommandCollection {
        type Command;
        fn add_to_cmd(self, &mut Vec<RespValue>);
        fn num_cmds(&self) -> usize;
    }

    impl<T: CommandAddable> CommandCollection for Vec<T> {
        type Command = T;

        fn add_to_cmd(self, cmd: &mut Vec<RespValue>) {
            for item in self {
                item.add_to_cmd(cmd);
            }
        }

        fn num_cmds(&self) -> usize {
            self.len() * T::num_cmds()
        }
    }

    impl<'a, T: CommandAddable + ToOwned<Owned = T>> CommandCollection for &'a [T] {
        type Command = T;

        fn add_to_cmd(self, cmd: &mut Vec<RespValue>) {
            for item in self {
                item.to_owned().add_to_cmd(cmd);
            }
        }

        fn num_cmds(&self) -> usize {
            self.len() * T::num_cmds()
        }
    }

    macro_rules! command_collection_ary {
        ($c:expr) => {
            impl<T: CommandAddable> CommandCollection for [T; $c] {
                type Command = T;

                fn add_to_cmd(mut self, cmd: &mut Vec<RespValue>) {
                    for idx in 0..$c {
                        let value = unsafe { mem::replace(&mut self[idx], mem::uninitialized()) };
                        value.add_to_cmd(cmd);
                    }
                }

                fn num_cmds(&self) -> usize {
                    $c * T::num_cmds()
                }
            }
        }
    }

    command_collection_ary!(1);
    command_collection_ary!(2);
    command_collection_ary!(3);
    command_collection_ary!(4);
    command_collection_ary!(5);
    command_collection_ary!(6);
    command_collection_ary!(7);
    command_collection_ary!(8);

    // TODO - check the expansion regarding trailing commas, etc.
    macro_rules! simple_command {
        ($n:ident,$k:expr,[ $(($p:ident : $t:ident)),+ ],$r:ty) => {
            pub fn $n< $($t),* >(&self, ($($p,)*): ($($t,)*)) -> SendBox<$r>
            where $($t: ToRespString + Into<RespValue>,)*
            {
                self.send(resp_array![ $k $(,$p)* ])
            }
        };
        ($n:ident,$k:expr,($p:ident : $t:ident),$r:ty) => {
            pub fn $n<$t>(&self, $p: ($t)) -> SendBox<$r>
            where $t: ToRespString + Into<RespValue>
            {
                self.send(resp_array![$k, $p])
            }
        };
        ($n:ident,$k:expr,$r:ty) => {
            pub fn $n(&self) -> SendBox<$r> {
                self.send(resp_array![$k])
            }
        };
    }

    macro_rules! bulk_str_command {
        ($n:ident,$k:expr,($p:ident : $t:ident)) => {
            pub fn $n<T, $t>(&self, $p: ($t)) -> SendBox<T>
            where $t: ToRespString + Into<RespValue>,
                  T: FromResp + 'static
            {
                self.send(resp_array![$k, $p])
            }
        }
    }

    impl super::PairedConnection {
        simple_command!(append, "APPEND", [(key: K), (value: V)], usize);
        simple_command!(auth, "AUTH", (password: P), ());
        simple_command!(bgrewriteaof, "BGREWRITEAOF", ());
        simple_command!(bgsave, "BGSAVE", ());
    }

    pub trait BitcountCommand {
        fn to_cmd(self) -> RespValue;
    }

    impl<T: ToRespString + Into<RespValue>> BitcountCommand for (T) {
        fn to_cmd(self) -> RespValue {
            resp_array!["BITCOUNT", self]
        }
    }

    impl<T: ToRespString + Into<RespValue>> BitcountCommand for (T, usize, usize) {
        fn to_cmd(self) -> RespValue {
            resp_array!["BITCOUNT", self.0, self.1.to_string(), self.2.to_string()]
        }
    }

    impl super::PairedConnection {
        pub fn bitcount<C>(&self, cmd: C) -> SendBox<usize>
            where C: BitcountCommand
        {
            self.send(cmd.to_cmd())
        }
    }

    pub struct BitfieldCommands {
        cmds: Vec<BitfieldCommand>,
    }

    #[derive(Clone)]
    pub enum BitfieldCommand {
        Set(BitfieldOffset, BitfieldTypeAndValue),
        Get(BitfieldOffset, BitfieldType),
        Incrby(BitfieldOffset, BitfieldTypeAndValue),
        Overflow(BitfieldOverflow),
    }

    impl BitfieldCommand {
        fn add_to_cmd(&self, cmds: &mut Vec<RespValue>) {
            match self {
                &BitfieldCommand::Set(ref offset, ref type_and_value) => {
                    cmds.push("SET".into());
                    cmds.push(type_and_value.type_cmd());
                    cmds.push(offset.to_cmd());
                    cmds.push(type_and_value.value_cmd());
                }
                &BitfieldCommand::Get(ref offset, ref ty) => {
                    cmds.push("GET".into());
                    cmds.push(ty.to_cmd());
                    cmds.push(offset.to_cmd());
                }
                &BitfieldCommand::Incrby(ref offset, ref type_and_value) => {
                    cmds.push("INCRBY".into());
                    cmds.push(type_and_value.type_cmd());
                    cmds.push(offset.to_cmd());
                    cmds.push(type_and_value.value_cmd());
                }
                &BitfieldCommand::Overflow(ref overflow) => {
                    cmds.push("OVERFLOW".into());
                    cmds.push(overflow.to_cmd());
                }
            }
        }
    }

    #[derive(Copy, Clone)]
    pub enum BitfieldType {
        Signed(usize),
        Unsigned(usize),
    }

    impl BitfieldType {
        fn to_cmd(&self) -> RespValue {
            match self {
                    &BitfieldType::Signed(size) => format!("i{}", size),
                    &BitfieldType::Unsigned(size) => format!("u{}", size),
                }
                .into()
        }
    }

    #[derive(Copy, Clone)]
    pub enum BitfieldOverflow {
        Wrap,
        Sat,
        Fail,
    }

    impl BitfieldOverflow {
        fn to_cmd(&self) -> RespValue {
            match self {
                    &BitfieldOverflow::Wrap => "WRAP",
                    &BitfieldOverflow::Sat => "SAT",
                    &BitfieldOverflow::Fail => "FAIL",
                }
                .into()
        }
    }

    #[derive(Clone)]
    pub enum BitfieldTypeAndValue {
        Signed(usize, isize),
        Unsigned(usize, usize),
    }

    impl BitfieldTypeAndValue {
        fn type_cmd(&self) -> RespValue {
            match self {
                    &BitfieldTypeAndValue::Signed(size, _) => format!("i{}", size),
                    &BitfieldTypeAndValue::Unsigned(size, _) => format!("u{}", size),
                }
                .into()
        }

        fn value_cmd(&self) -> RespValue {
            match self {
                    &BitfieldTypeAndValue::Signed(_, amt) => amt.to_string(),
                    &BitfieldTypeAndValue::Unsigned(_, amt) => amt.to_string(),
                }
                .into()
        }
    }

    #[derive(Clone)]
    pub enum BitfieldOffset {
        Bits(usize),
        Positional(usize),
    }

    impl BitfieldOffset {
        fn to_cmd(&self) -> RespValue {
            match self {
                    &BitfieldOffset::Bits(size) => size.to_string(),
                    &BitfieldOffset::Positional(size) => format!("#{}", size),
                }
                .into()
        }
    }

    impl BitfieldCommands {
        pub fn new() -> Self {
            BitfieldCommands { cmds: Vec::new() }
        }

        pub fn set(&mut self, offset: BitfieldOffset, value: BitfieldTypeAndValue) -> &mut Self {
            self.cmds.push(BitfieldCommand::Set(offset, value));
            self
        }

        pub fn get(&mut self, offset: BitfieldOffset, ty: BitfieldType) -> &mut Self {
            self.cmds.push(BitfieldCommand::Get(offset, ty));
            self
        }

        pub fn incrby(&mut self, offset: BitfieldOffset, value: BitfieldTypeAndValue) -> &mut Self {
            self.cmds.push(BitfieldCommand::Incrby(offset, value));
            self
        }

        pub fn overflow(&mut self, overflow: BitfieldOverflow) -> &mut Self {
            self.cmds.push(BitfieldCommand::Overflow(overflow));
            self
        }

        fn to_cmd(&self, key: RespValue) -> RespValue {
            let mut cmd = Vec::new();
            cmd.push("BITFIELD".into());
            cmd.push(key);
            for subcmd in self.cmds.iter() {
                subcmd.add_to_cmd(&mut cmd);
            }
            RespValue::Array(cmd)
        }
    }

    impl super::PairedConnection {
        pub fn bitfield<K>(&self, (key, cmds): (K, &BitfieldCommands)) -> SendBox<Vec<Option<i64>>>
            where K: ToRespString + Into<RespValue>
        {
            self.send(cmds.to_cmd(key.into()))
        }
    }

    #[derive(Copy, Clone)]
    pub enum BitOp {
        And,
        Or,
        Xor,
        Not,
    }

    impl From<BitOp> for RespValue {
        fn from(op: BitOp) -> RespValue {
            match op {
                    BitOp::And => "AND",
                    BitOp::Or => "OR",
                    BitOp::Xor => "XOR",
                    BitOp::Not => "NOT",
                }
                .into()
        }
    }

    impl super::PairedConnection {
        pub fn bitop<K, T, C>(&self, (op, destkey, keys): (BitOp, K, C)) -> SendBox<i64>
            where K: ToRespString + Into<RespValue>,
                  T: ToRespString + Into<RespValue>,
                  C: CommandCollection<Command = T>
        {
            let key_len = keys.num_cmds();
            if key_len == 0 {
                return Box::new(future::err(error::internal("BITOP command needs at least one key",),),);
            }

            let mut cmd = Vec::with_capacity(2 + key_len);
            cmd.push(op.into());
            cmd.push(destkey.into());
            keys.add_to_cmd(&mut cmd);

            self.send(RespValue::Array(cmd))
        }
    }

    pub trait BitposCommand {
        fn to_cmd(self) -> RespValue;
    }

    impl<K, B> BitposCommand for (K, B, usize)
        where K: ToRespString + Into<RespValue>,
              B: ToRespString + Into<RespValue>
    {
        fn to_cmd(self) -> RespValue {
            resp_array!["BITPOS", self.0, self.1, self.2.to_string()]
        }
    }

    impl<K, B> BitposCommand for (K, B, usize, usize)
        where K: ToRespString + Into<RespValue>,
              B: ToRespString + Into<RespValue>
    {
        fn to_cmd(self) -> RespValue {
            resp_array!["BITPOS",
                        self.0,
                        self.1,
                        self.2.to_string(),
                        self.3.to_string()]
        }
    }

    impl super::PairedConnection {
        pub fn bitpos<C>(&self, cmd: C) -> SendBox<i64>
            where C: BitposCommand
        {
            self.send(cmd.to_cmd())
        }
    }

    impl super::PairedConnection {
        pub fn blpop<C, T, K, V>(&self, (keys, timeout): (C, usize)) -> SendBox<Option<(K, V)>>
            where C: CommandCollection<Command = T>,
                  T: ToRespString + Into<RespValue>,
                  K: FromResp + 'static,
                  V: FromResp + 'static
        {
            let keys_len = keys.num_cmds();
            if keys_len == 0 {
                return Box::new(future::err(error::internal("BLPOP requires at least one key")));
            }

            let mut cmd = Vec::with_capacity(2 + keys_len);
            cmd.push("BLPOP".into());
            keys.add_to_cmd(&mut cmd);
            cmd.push(timeout.to_string().into());
            self.send(RespValue::Array(cmd))
        }
    }

    impl super::PairedConnection {
        pub fn brpop<C, T, K, V>(&self, (keys, timeout): (C, usize)) -> SendBox<Option<(K, V)>>
            where C: CommandCollection<Command = T>,
                  T: ToRespString + Into<RespValue>,
                  K: FromResp + 'static,
                  V: FromResp + 'static
        {
            let keys_len = keys.num_cmds();
            if keys_len == 0 {
                return Box::new(future::err(error::internal("BRPOP requires at least one key")));
            }

            let mut cmd = Vec::new();
            cmd.push("BRPOP".into());
            keys.add_to_cmd(&mut cmd);
            cmd.push(timeout.to_string().into());
            self.send(RespValue::Array(cmd))
        }
    }

    impl super::PairedConnection {
        pub fn brpoplpush<S, V, T>(&self,
                                   (source, destination, timeout): (S, V, usize))
                                   -> SendBox<Option<T>>
            where S: ToRespString + Into<RespValue>,
                  V: ToRespString + Into<RespValue>,
                  T: FromResp + 'static
        {
            self.send(resp_array!["BRPOPLPUSH", source, destination, timeout.to_string()])
        }
    }

    // TODO - implement the CLIENT commands
    // TODO - implement the CLUSTER commands
    // TODO - implement the COMMAND commands

    impl super::PairedConnection {
        simple_command!(dbsize, "DBSIZE", usize);
        simple_command!(decr, "DECR", (key: K), i64);
    }

    impl super::PairedConnection {
        pub fn decrby<K>(&self, (key, increment): (K, i64)) -> SendBox<i64>
            where K: ToRespString + Into<RespValue>
        {
            self.send(resp_array!["DECRBY", key, increment.to_string()])
        }
    }

    impl super::PairedConnection {
        pub fn del<C, T>(&self, keys: (C)) -> SendBox<usize>
            where C: CommandCollection<Command = T>,
                  T: ToRespString + Into<RespValue>
        {
            let keys_len = keys.num_cmds();
            if keys_len == 0 {
                return Box::new(future::err(error::internal("DEL command needs at least one key")));
            }

            let mut cmd = Vec::with_capacity(1 + keys_len);
            cmd.push("DEL".into());
            keys.add_to_cmd(&mut cmd);

            self.send(RespValue::Array(cmd))
        }
    }

    impl super::PairedConnection {
        simple_command!(dump, "DUMP", (key: K), Option<Vec<u8>>);
        bulk_str_command!(echo, "ECHO", (msg: M));
    }

    impl super::PairedConnection {
        pub fn eval<S, K, KT, A, AT, T>(&self,
                                        (script, keys, args): (S, Option<K>, Option<A>))
                                        -> SendBox<T>
            where S: ToRespString + Into<RespValue>,
                  K: CommandCollection<Command = KT>,
                  KT: ToRespString + Into<RespValue>,
                  A: CommandCollection<Command = KT>,
                  AT: ToRespString + Into<RespValue>,
                  T: FromResp + 'static
        {
            let keys_len = keys.as_ref().map(|k| k.num_cmds()).unwrap_or(0);
            let args_len = args.as_ref().map(|a| a.num_cmds()).unwrap_or(0);

            let mut cmd = Vec::with_capacity(3 + keys_len + args_len);
            cmd.push("EVAL".into());
            cmd.push(script.into());
            cmd.push(keys_len.to_string().into());

            if keys.is_some() {
                keys.unwrap().add_to_cmd(&mut cmd);
            }
            if args.is_some() {
                args.unwrap().add_to_cmd(&mut cmd);
            }

            self.send(RespValue::Array(cmd))
        }

        pub fn evalsha<S, K, KT, A, AT, T>(&self,
                                           (sha, keys, args): (S, Option<K>, Option<A>))
                                           -> SendBox<T>
            where S: ToRespString + Into<RespValue>,
                  K: CommandCollection<Command = KT>,
                  KT: ToRespString + Into<RespValue>,
                  A: CommandCollection<Command = AT>,
                  AT: ToRespString + Into<RespValue>,
                  T: FromResp + 'static
        {
            let keys_len = keys.as_ref().map(|k| k.num_cmds()).unwrap_or(0);
            let args_len = args.as_ref().map(|a| a.num_cmds()).unwrap_or(0);

            let mut cmd = Vec::with_capacity(3 + keys_len + args_len);
            cmd.push("EVALSHA".into());
            cmd.push(sha.into());
            cmd.push(keys_len.to_string().into());

            if keys.is_some() {
                keys.unwrap().add_to_cmd(&mut cmd);
            }
            if args.is_some() {
                args.unwrap().add_to_cmd(&mut cmd);
            }

            self.send(RespValue::Array(cmd))
        }
    }

    impl super::PairedConnection {
        pub fn exists<C, T>(&self, keys: (C)) -> SendBox<usize>
            where C: CommandCollection<Command = T>,
                  T: ToRespString + Into<RespValue>
        {
            let keys_len = keys.num_cmds();
            let mut cmd = Vec::with_capacity(1 + keys_len);
            cmd.push("EXISTS".into());
            keys.add_to_cmd(&mut cmd);

            self.send(RespValue::Array(cmd))
        }
    }

    pub enum ExpireResult {
        Found,
        NotFound,
    }

    impl ExpireResult {
        pub fn found(&self) -> bool {
            match self {
                &ExpireResult::Found => true,
                &ExpireResult::NotFound => false,
            }
        }
    }

    impl FromResp for ExpireResult {
        fn from_resp_int(resp: RespValue) -> Result<Self, error::Error> {
            match resp {
                RespValue::Integer(0) => Ok(ExpireResult::NotFound),
                RespValue::Integer(1) => Ok(ExpireResult::Found),
                _ => Err(error::resp("Expecting 0 or 1", resp)),
            }
        }
    }

    impl super::PairedConnection {
        pub fn expire<K>(&self, (key, seconds): (K, usize)) -> SendBox<ExpireResult>
            where K: ToRespString + Into<RespValue>
        {
            self.send(resp_array!["EXPIRE", key, seconds.to_string()])
        }

        pub fn expireat<K>(&self, (key, timestamp): (K, usize)) -> SendBox<ExpireResult>
            where K: ToRespString + Into<RespValue>
        {
            self.send(resp_array!["EXPIREAT", key, timestamp.to_string()])
        }
    }

    pub trait FlushallCommand {
        fn to_cmd(self) -> RespValue;
    }

    pub trait FlushdbCommand {
        fn to_cmd(self) -> RespValue;
    }

    impl FlushallCommand for () {
        fn to_cmd(self) -> RespValue {
            resp_array!["FLUSHALL"]
        }
    }

    impl FlushdbCommand for () {
        fn to_cmd(self) -> RespValue {
            resp_array!["FLUSHDB"]
        }
    }

    #[allow(dead_code)]
    pub struct Async;

    impl FlushallCommand for (Async) {
        fn to_cmd(self) -> RespValue {
            resp_array!["FLUSHALL", "ASYNC"]
        }
    }

    impl FlushdbCommand for (Async) {
        fn to_cmd(self) -> RespValue {
            resp_array!["FLUSHDB", "ASYNC"]
        }
    }

    impl super::PairedConnection {
        pub fn flushall<F>(&self, cmd: F) -> SendBox<()>
            where F: FlushallCommand
        {
            self.send(cmd.to_cmd())
        }

        pub fn flushdb<F>(&self, cmd: F) -> SendBox<()>
            where F: FlushdbCommand
        {
            self.send(cmd.to_cmd())
        }
    }

    fn parse_f64(val: String) -> Result<f64, error::Error> {
        val.parse()
            .map_err(|_| {
                         error::Error::RESP(format!("Expected float as String, got: {}", val), None)
                     })
    }

    #[derive(Clone, Copy)]
    pub enum GeoUnit {
        M,
        Km,
        Ft,
        Mi,
    }

    impl GeoUnit {
        fn as_str(&self) -> &str {
            use self::GeoUnit::*;
            match *self {
                M => "m",
                Km => "km",
                Ft => "ft",
                Mi => "mi",
            }
        }
    }

    impl Default for GeoUnit {
        fn default() -> Self {
            GeoUnit::Km
        }
    }

    impl super::PairedConnection {
        pub fn geoadd<K, C, T>(&self, (key, details): (K, C)) -> SendBox<usize>
            where K: ToRespString + Into<RespValue>,
                  C: CommandCollection<Command = (f64, f64, T)>,
                  T: ToRespString + Into<RespValue>
        {
            let keys_len = details.num_cmds();
            if keys_len == 0 {
                return Box::new(future::err(error::internal("GEOADD command needs at least one key",),),);
            }

            let mut cmd = Vec::with_capacity(2 + keys_len);
            cmd.push("GEOADD".into());
            cmd.push(key.into());
            details.add_to_cmd(&mut cmd);

            self.send(RespValue::Array(cmd))
        }

        pub fn geohash<K, C, T>(&self, (key, members): (K, C)) -> SendBox<Vec<Option<String>>>
            where K: ToRespString + Into<RespValue>,
                  C: CommandCollection<Command = T>,
                  T: ToRespString + Into<RespValue>
        {
            let keys_len = members.num_cmds();
            if keys_len == 0 {
                return Box::new(future::err(error::internal("GEOHASH needs at least one member")));
            }

            let mut cmd = Vec::with_capacity(2 + keys_len);
            cmd.push("GEOHASH".into());
            cmd.push(key.into());
            members.add_to_cmd(&mut cmd);

            self.send(RespValue::Array(cmd))
        }

        pub fn geopos<K, C, T>(&self, (key, members): (K, C)) -> SendBox<Vec<Option<(f64, f64)>>>
            where K: ToRespString + Into<RespValue>,
                  C: CommandCollection<Command = T>,
                  T: ToRespString + Into<RespValue>
        {
            let keys_len = members.num_cmds();
            if keys_len == 0 {
                return Box::new(future::err(error::internal("GEOPOS needs at least one member")));
            }

            let mut cmd = Vec::with_capacity(2 + keys_len);
            cmd.push("GEOPOS".into());
            cmd.push(key.into());
            members.add_to_cmd(&mut cmd);

            let parsed = self.send(RespValue::Array(cmd))
                .and_then(|response: Vec<Option<(String, String)>>| {
                    let mut parsed = Vec::with_capacity(response.len());
                    for r in response {
                        parsed.push(match r {
                                        Some((lng_s, lat_s)) => {
                                            let lng = parse_f64(lng_s);
                                            if lng.is_err() {
                                                return future::err(lng.unwrap_err());
                                            }
                                            let lat = parse_f64(lat_s);
                                            if lat.is_err() {
                                                return future::err(lat.unwrap_err());
                                            }
                                            Some((lng.unwrap(), lat.unwrap()))
                                        }
                                        None => None,
                                    });
                    }
                    future::ok(parsed)
                });

            Box::new(parsed)
        }
    }

    pub trait GeodistCommand {
        fn to_cmd(self) -> RespValue;
    }

    impl<K, M1, M2> GeodistCommand for (K, M1, M2)
        where K: ToRespString + Into<RespValue>,
              M1: ToRespString + Into<RespValue>,
              M2: ToRespString + Into<RespValue>
    {
        fn to_cmd(self) -> RespValue {
            resp_array!["GEODIST", self.0, self.1, self.2]
        }
    }

    impl<K, M1, M2> GeodistCommand for (K, M1, M2, GeoUnit)
        where K: ToRespString + Into<RespValue>,
              M1: ToRespString + Into<RespValue>,
              M2: ToRespString + Into<RespValue>
    {
        fn to_cmd(self) -> RespValue {
            resp_array!["GEODIST", self.0, self.1, self.2, self.3.as_str()]
        }
    }

    impl super::PairedConnection {
        pub fn geodist<C>(&self, cmd: C) -> SendBox<Option<f64>>
            where C: GeodistCommand
        {
            let parsed = self.send(cmd.to_cmd())
                .and_then(|response: Option<String>| match response {
                              Some(string) => {
                                  match parse_f64(string) {
                                      Ok(dist) => future::ok(Some(dist)),
                                      Err(e) => future::err(e),
                                  }
                              }
                              None => future::ok(None),
                          });

            Box::new(parsed)
        }
    }

    #[derive(Clone, Copy)]
    pub enum GeoradiusOrder {
        Asc,
        Desc,
    }

    impl GeoradiusOrder {
        fn as_str(&self) -> &str {
            use self::GeoradiusOrder::*;
            match *self {
                Asc => "asc",
                Desc => "desc",
            }
        }
    }

    #[derive(Clone)]
    pub enum GeoradiusLocation<T> {
        LngLat(f64, f64),
        Member(T),
    }

    impl<T> Default for GeoradiusLocation<T> {
        fn default() -> Self {
            GeoradiusLocation::LngLat(0.0, 0.0)
        }
    }

    #[derive(Clone, Default)]
    pub struct GeoradiusOptions<K>
        where K: ToRespString + Into<RespValue> + Default
    {
        pub key: K,
        pub location: GeoradiusLocation<K>,
        pub radius: f64,
        pub units: GeoUnit,
        pub withcoord: bool,
        pub withdist: bool,
        pub withhash: bool,
        pub count: Option<usize>,
        pub order: Option<GeoradiusOrder>,
        pub store: Option<K>,
        pub storedist: Option<K>,
    }

    struct GeoradiusParseOptions {
        withcoord: bool,
        withdist: bool,
        withhash: bool,
    }

    impl<K> GeoradiusOptions<K>
        where K: ToRespString + Into<RespValue> + Default
    {
        fn parse_options(&self) -> GeoradiusParseOptions {
            GeoradiusParseOptions {
                withcoord: self.withcoord,
                withdist: self.withdist,
                withhash: self.withhash,
            }
        }

        pub fn withcoord(&mut self) -> &mut Self {
            self.withcoord = true;
            self
        }

        pub fn withdist(&mut self) -> &mut Self {
            self.withdist = true;
            self
        }

        pub fn withhash(&mut self) -> &mut Self {
            self.withhash = true;
            self
        }

        pub fn count(&mut self, count: usize) -> &mut Self {
            self.count = Some(count);
            self
        }

        pub fn order(&mut self, order: GeoradiusOrder) -> &mut Self {
            self.order = Some(order);
            self
        }

        pub fn store(&mut self, store: K) -> &mut Self {
            self.store = Some(store);
            self
        }

        pub fn storedist(&mut self, storedist: K) -> &mut Self {
            self.storedist = Some(storedist);
            self
        }

        fn to_cmd(self) -> RespValue {
            let mut cmd = Vec::new();
            match self.location {
                GeoradiusLocation::LngLat(_, _) => cmd.push("GEORADIUS".into()),
                GeoradiusLocation::Member(_) => cmd.push("GEORADIUSBYMEMBER".into()),
            }
            cmd.push(self.key.into());
            match self.location {
                GeoradiusLocation::LngLat(lng, lat) => {
                    cmd.push(lng.to_string().into());
                    cmd.push(lat.to_string().into());
                }
                GeoradiusLocation::Member(string) => cmd.push(string.into()),
            }
            cmd.push(self.radius.to_string().into());
            cmd.push(self.units.as_str().into());
            if self.withcoord {
                cmd.push("WITHCOORD".into());
            }
            if self.withdist {
                cmd.push("WITHDIST".into());
            }
            if self.withhash {
                cmd.push("WITHHASH".into());
            }
            if let Some(count) = self.count {
                cmd.push("COUNT".into());
                cmd.push(count.to_string().into());
            }
            if let Some(order) = self.order {
                cmd.push(order.as_str().into());
            }
            if let Some(store) = self.store {
                cmd.push("STORE".into());
                cmd.push(store.into());
            }
            if let Some(storedist) = self.storedist {
                cmd.push("STOREDIST".into());
                cmd.push(storedist.into());
            }

            RespValue::Array(cmd)
        }
    }

    impl GeoradiusParseOptions {
        fn prepare_result(&self, resp: RespValue) -> Result<GeoradiusResponse, error::Error> {
            match resp {
                RespValue::Array(mut ary) => {
                    let mut idx = ary.len() - 1;
                    let coord = if self.withcoord {
                        let (lng, lat): (String, String) = FromResp::from_resp(ary.remove(idx))?;
                        idx -= 1;
                        Some((parse_f64(lng)?, parse_f64(lat)?))
                    } else {
                        None
                    };
                    let hash = if self.withhash {
                        let h = FromResp::from_resp(ary.remove(idx))?;
                        idx -= 1;
                        Some(h)
                    } else {
                        None
                    };
                    let dist = if self.withdist {
                        let d = FromResp::from_resp(ary.remove(idx))?;
                        idx -= 1;
                        Some(parse_f64(d)?)
                    } else {
                        None
                    };
                    let member = FromResp::from_resp(ary.remove(idx))?;
                    Ok(GeoradiusResponse {
                           member: member,
                           dist: dist,
                           hash: hash,
                           coord: coord,
                       })
                }
                _ => {
                    Err(error::Error::RESP(String::from("Not an array, cannot read an element of a GEORADIUS response",),
                                           Some(resp)))
                }
            }
        }

        fn prepare_response(&self,
                            resp: RespValue)
                            -> Result<Vec<GeoradiusResponse>, error::Error> {
            if let RespValue::Array(ary) = resp {
                let mut response = Vec::with_capacity(ary.len());
                for resp in ary {
                    response.push(self.prepare_result(resp)?);
                }
                Ok(response)
            } else {
                Err(error::Error::RESP(String::from("Not an array, cannot read a GEORADIUS response",),
                                       Some(resp)))
            }
        }
    }

    impl<K> From<(K, f64, f64, f64, GeoUnit)> for GeoradiusOptions<K>
        where K: ToRespString + Into<RespValue> + Default
    {
        fn from((key, lng, lat, radius, units): (K, f64, f64, f64, GeoUnit)) -> Self {
            GeoradiusOptions {
                key: key,
                location: GeoradiusLocation::LngLat(lng, lat),
                radius: radius,
                units: units,
                ..Default::default()
            }
        }
    }

    impl<K> From<(K, K, f64, GeoUnit)> for GeoradiusOptions<K>
        where K: ToRespString + Into<RespValue> + Default
    {
        fn from((key, member, radius, units): (K, K, f64, GeoUnit)) -> Self {
            GeoradiusOptions {
                key: key,
                location: GeoradiusLocation::Member(member),
                radius: radius,
                units: units,
                ..Default::default()
            }
        }
    }

    pub struct GeoradiusResponse {
        pub member: String,
        pub dist: Option<f64>,
        pub hash: Option<usize>,
        pub coord: Option<(f64, f64)>,
    }

    impl super::PairedConnection {
        pub fn georadius<O, K>(&self, options: O) -> SendBox<Vec<GeoradiusResponse>>
            where O: Into<GeoradiusOptions<K>>,
                  K: ToRespString + Into<RespValue> + Default + 'static
        {
            let options = options.into();
            let parse_options = options.parse_options();
            let parsed_future = self.send(options.to_cmd())
                .and_then(move |resp: RespValue| {
                              future::result(parse_options.prepare_response(resp))
                          });
            Box::new(parsed_future)
        }

        pub fn georadiusbymember<O, K>(&self, options: O) -> SendBox<Vec<GeoradiusResponse>>
            where O: Into<GeoradiusOptions<K>>,
                  K: ToRespString + Into<RespValue> + Default + 'static
        {
            self.georadius(options)
        }
    }

    impl super::PairedConnection {
        pub fn get<K, T>(&self, key: (K)) -> SendBox<Option<T>>
            where K: ToRespString + Into<RespValue>,
                  T: FromResp + 'static
        {
            self.send(resp_array!["GET", key])
        }
    }

    impl super::PairedConnection {
        pub fn getbit<K>(&self, (key, offset): (K, usize)) -> SendBox<usize>
            where K: ToRespString + Into<RespValue>
        {
            self.send(resp_array!["GETBIT", key, offset.to_string()])
        }
    }

    // MARKER - all accounted for above this line

    impl super::PairedConnection {
        simple_command!(incr, "INCR", (key: K), i64);
    }

    impl super::PairedConnection {
        // TODO: incomplete implementation
        pub fn set<K, V>(&self, (key, value): (K, V)) -> SendBox<()>
            where K: ToRespString + Into<RespValue>,
                  V: ToRespString + Into<RespValue>
        {
            self.send(resp_array!["SET", key, value])
        }
    }

    #[cfg(test)]
    mod test {
        use futures::future;
        use futures::Future;

        use tokio_core::reactor::Core;

        use super::{BitfieldCommands, BitfieldTypeAndValue, BitfieldOffset, BitfieldOverflow,
                    GeoradiusOptions, GeoradiusOrder, GeoUnit};

        use super::super::error::Error;

        fn setup() -> (Core, super::super::PairedConnectionBox) {
            let core = Core::new().unwrap();
            let handle = core.handle();
            let addr = "127.0.0.1:6379".parse().unwrap();

            (core, super::super::paired_connect(&addr, &handle))
        }

        fn setup_and_delete(keys: Vec<&str>) -> (Core, super::super::PairedConnectionBox) {
            let (mut core, connection) = setup();

            let delete = connection.and_then(|connection| connection.del(keys).map(|_| connection));

            let connection = core.run(delete).unwrap();
            (core, Box::new(future::ok(connection)))
        }

        #[test]
        fn append_test() {
            let (mut core, connection) = setup_and_delete(vec!["APPENDKEY"]);

            let connection = connection
                .and_then(|connection| connection.append(("APPENDKEY", "ABC")));

            let count = core.run(connection).unwrap();
            assert_eq!(count, 3);
        }

        #[test]
        fn bitcount_test() {
            let (mut core, connection) = setup();

            let connection = connection.and_then(|connection| {
                connection
                    .set(("BITCOUNT_KEY", "foobar"))
                    .and_then(move |_| {
                                  let mut counts = Vec::new();
                                  counts.push(connection.bitcount("BITCOUNT_KEY"));
                                  counts.push(connection.bitcount(("BITCOUNT_KEY", 0, 0)));
                                  counts.push(connection.bitcount(("BITCOUNT_KEY", 1, 1)));
                                  future::join_all(counts)
                              })
            });

            let counts = core.run(connection).unwrap();
            assert_eq!(counts.len(), 3);
            assert_eq!(counts[0], 26);
            assert_eq!(counts[1], 4);
            assert_eq!(counts[2], 6);
        }

        #[test]
        fn bitfield_test() {
            let (mut core, connection) = setup_and_delete(vec!["BITFIELD_KEY"]);

            let connection = connection.and_then(|connection| {
                let mut bitfield_commands = BitfieldCommands::new();
                bitfield_commands.incrby(BitfieldOffset::Bits(100),
                                         BitfieldTypeAndValue::Unsigned(2, 1));
                bitfield_commands.overflow(BitfieldOverflow::Sat);
                bitfield_commands.incrby(BitfieldOffset::Bits(102),
                                         BitfieldTypeAndValue::Unsigned(2, 1));

                connection.bitfield(("BITFIELD_KEY", &bitfield_commands))
            });

            let results = core.run(connection).unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0], Some(1));
            assert_eq!(results[1], Some(1));
        }

        #[test]
        fn bitfield_nil_response() {
            let (mut core, connection) = setup_and_delete(vec!["BITFIELD_NIL_KEY"]);

            let connection = connection.and_then(|connection| {
                let mut bitfield_commands = BitfieldCommands::new();
                bitfield_commands.overflow(BitfieldOverflow::Fail);
                bitfield_commands.incrby(BitfieldOffset::Bits(102),
                                         BitfieldTypeAndValue::Unsigned(2, 4));
                connection.bitfield(("BITFIELD_NIL_KEY", &bitfield_commands))
            });

            let results = core.run(connection).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], None);
        }

        #[test]
        fn decr_test() {
            let (mut core, connection) = setup_and_delete(vec!["DECR_KEY"]);

            let connection =
                connection.and_then(|connection| {
                                        connection
                                            .set(("DECR_KEY", "123"))
                                            .and_then(move |_| connection.decr("DECR_KEY"))
                                    });

            let result = core.run(connection).unwrap();
            assert_eq!(result, 122);
        }

        #[test]
        fn decrby_test() {
            let (mut core, connection) = setup_and_delete(vec!["DECRBY_KEY"]);

            let connection = connection.and_then(|connection| {
                connection
                    .set(("DECRBY_KEY", "555"))
                    .and_then(move |_| connection.decrby(("DECRBY_KEY", 43)))
            });

            let result = core.run(connection).unwrap();
            assert_eq!(result, 512);
        }

        #[test]
        fn del_test_vec() {
            let (mut core, connection) = setup();

            let del_keys = vec!["DEL_KEY_1", "DEL_KEY_2"];
            let connection = connection.and_then(|connection| connection.del(del_keys));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn del_test_vec_string() {
            let (mut core, connection) = setup();

            let del_keys = vec![String::from("DEL_KEY_1"), String::from("DEL_KEY_2")];
            let connection = connection.and_then(|connection| connection.del(del_keys));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn del_test_slice() {
            let (mut core, connection) = setup();

            let del_keys = ["DEL_KEY_1", "DEL_KEY_2"];
            let connection = connection.and_then(|connection| connection.del(&del_keys[..]));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn del_test_slice_string() {
            let (mut core, connection) = setup();

            let del_keys = [String::from("DEL_KEY_1"), String::from("DEL_KEY_2")];
            let connection = connection.and_then(|connection| connection.del(&del_keys[..]));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn del_test_ary() {
            let (mut core, connection) = setup();

            let del_keys = ["DEL_KEY_1"];
            let connection = connection.and_then(|connection| connection.del(del_keys));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn del_test_ary2() {
            let (mut core, connection) = setup();

            let del_keys = ["DEL_KEY_1", "DEL_KEY_2"];
            let connection = connection.and_then(|connection| connection.del(del_keys));

            let _ = core.run(connection).unwrap();
        }

        #[test]
        fn del_not_enough_keys() {
            let (mut core, connection) = setup();

            let del_keys: Vec<String> = vec![];
            let connection = connection.and_then(|connection| connection.del(del_keys));

            let result = core.run(connection);
            if let &Err(Error::Internal(ref msg)) = &result {
                assert_eq!("DEL command needs at least one key", msg);
            } else {
                panic!("Should have errored: {:?}", result);
            }
        }

        #[test]
        fn dump_test() {
            let (mut core, connection) = setup_and_delete(vec!["DUMP_TEST"]);

            let connection =
                connection.and_then(|connection| {
                                        connection
                                            .set(("DUMP_TEST", "123"))
                                            .and_then(move |_| connection.dump("DUMP_TEST"))
                                    });

            let result = core.run(connection).unwrap();
            let _: Vec<u8> = result.unwrap();
        }

        #[test]
        fn dump_nil_test() {
            let (mut core, connection) = setup_and_delete(vec!["DUMP_NIL_TEST"]);

            let connection = connection.and_then(|connection| connection.dump("DUMP_NIL_TEST"));

            let result = core.run(connection).unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn geoadd_test() {
            let (mut core, connection) = setup_and_delete(vec!["GEOADD_TEST"]);
            let connection = connection.and_then(|connection| {
                connection.geoadd(("GEOADD_TEST",
                                   [(13.361389, 38.115556, "Palermo"),
                                    (15.087269, 37.502669, "Catania")]))
            });

            let result = core.run(connection).unwrap();
            assert_eq!(result, 2);
        }

        #[test]
        fn geohash_test() {
            let (mut core, connection) = setup_and_delete(vec!["GEOHASH_TEST"]);
            let connection = connection.and_then(|connection| {
                connection
                    .geoadd(("GEOHASH_TEST",
                             [(13.361389, 38.115556, "Palermo"),
                              (15.087269, 37.502669, "Catania")]))
                    .and_then(move |_| {
                                  connection.geohash(("GEOHASH_TEST",
                                                      ["Palermo", "Noway", "Catania"]))
                              })
            });

            let result = core.run(connection).unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(result[0], Some(String::from("sqc8b49rny0")));
            assert_eq!(result[1], None);
            assert_eq!(result[2], Some(String::from("sqdtr74hyu0")));
        }

        #[test]
        fn georadius_test() {
            let (mut core, connection) = setup_and_delete(vec!["GEORADIUS_TEST"]);
            let connection = connection.and_then(|connection| {
                connection
                    .geoadd(("GEORADIUS_TEST",
                             [(13.361389, 38.115556, "Palermo"),
                              (15.087269, 37.502669, "Catania")]))
                    .and_then(move |_| {
                        let mut options: GeoradiusOptions<&str> =
                            ("GEORADIUS_TEST", 15.0, 37.0, 200.0, GeoUnit::Km).into();
                        options.order(GeoradiusOrder::Desc);

                        let mut withdist_opts = options.clone();
                        withdist_opts.withdist();
                        let withdist = connection.georadius(withdist_opts);

                        let mut withcoord_opts = options.clone();
                        withcoord_opts.withcoord();
                        let withcoord = connection.georadius(withcoord_opts);

                        let mut withboth_opts = options.clone();
                        withboth_opts.withdist();
                        withboth_opts.withcoord();
                        let withboth = connection.georadius(withboth_opts);

                        future::join_all(vec![withdist, withcoord, withboth])
                    })
            });

            let results = core.run(connection).unwrap();
            assert_eq!(results.len(), 3);

            assert_eq!(results[0].len(), 2);
            assert_eq!(results[0][0].member, "Palermo");
            assert_eq!(results[0][0].dist, Some(190.4424));
            assert_eq!(results[0][0].coord, None);
            assert_eq!(results[0][0].hash, None);
            assert_eq!(results[0][1].member, "Catania");

            assert_eq!(results[1].len(), 2);
            assert_eq!(results[1][0].member, "Palermo");
            assert_eq!(results[1][0].dist, None);
            assert_eq!(results[1][0].coord, Some((13.36138933897018433, 38.11555639549629859)));

            assert_eq!(results[2].len(), 2);
            assert_eq!(results[2][0].member, "Palermo");
            assert_eq!(results[2][0].dist, Some(190.4424));
            assert_eq!(results[2][0].coord, Some((13.36138933897018433, 38.11555639549629859)));
        }
    }
}
