/*
 * Copyright 2017 Ben Ashford
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

#![feature(test)]

extern crate test;

extern crate futures;
extern crate tokio_core;

#[macro_use]
extern crate redis_async;

use std::sync::Arc;

use test::Bencher;

use futures::Future;

use tokio_core::reactor::Core;

use redis_async::client;

#[bench]
fn bench_simple_getsetdel(b: &mut Bencher) {
    let mut core = Core::new().unwrap();
    let addr = "127.0.0.1:6379".parse().unwrap();

    let connection = client::paired_connect(&addr, &core.handle());
    let connection = core.run(connection).unwrap();

    b.iter(|| {
        faf!(connection.send(resp_array!["SET", "test_key", "42"]));
        let get = connection.send(resp_array!["GET", "test_key"]);
        let del = connection.send(resp_array!["DEL", "test_key"]);
        let get_set = get.join(del);
        let (_, _): (String, usize) = core.run(get_set).unwrap();
    });
}

#[bench]
fn bench_big_pipeline(b: &mut Bencher) {
    let mut core = Core::new().unwrap();
    let addr = "127.0.0.1:6379".parse().unwrap();

    let connection = client::paired_connect(&addr, &core.handle());
    let connection = core.run(connection).unwrap();

    let data_size = 100;

    b.iter(|| {
        for x in 0..data_size {
            let test_key = format!("test_{}", x);
            faf!(connection.send(resp_array!["SET", test_key, x.to_string()]));
        }
        let mut gets = Vec::with_capacity(data_size);
        for x in 0..data_size {
            let test_key = format!("test_{}", x);
            gets.push(connection.send(resp_array!["GET", test_key]));
        }
        let last_get = gets.remove(data_size - 1);
        let _: String = core.run(last_get).unwrap();
    });
}

#[bench]
fn bench_complex_pipeline(b: &mut Bencher) {
    let mut core = Core::new().unwrap();
    let addr = "127.0.0.1:6379".parse().unwrap();

    let connection = client::paired_connect(&addr, &core.handle());
    let connection = Arc::new(core.run(connection).unwrap());

    let data_size = 100;

    b.iter(|| {
        let sets = (0..data_size).map(|x| {
            let connection_inner = connection.clone();
            connection
                .send(resp_array!["INCR", "id_gen"])
                .and_then(move |id: i64| {
                    let id = format!("id_{}", id);
                    connection_inner.send(resp_array!["SET", id, x.to_string()])
                })
        });
        let all_sets = futures::future::join_all(sets);
        let _: Vec<String> = core.run(all_sets).unwrap();
    });
}
