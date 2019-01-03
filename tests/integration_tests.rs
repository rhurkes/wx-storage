extern crate wx_storage;

use bincode::{deserialize, serialize};
use rocksdb::{DB, Options};
use std::{str, thread, time};
use wx::EventMessage;
use wx_storage::Store;
use zmq::Message;

const THRESHOLD_MICROS: u64 = 1000 * 1000 * 60 * 60;    // 1 hr
const TEST_STORE_PATH: &str = "wx_test";

#[test]
fn zero_message_length_should_error() {
    let store = Store::new(TEST_STORE_PATH, THRESHOLD_MICROS);
    let msg = Message::new();
    let result = wx_storage::process_msg(&msg, &store);
    assert!(result.is_err())
}

#[test]
fn unknown_command_should_error() {
    let store = Store::new(TEST_STORE_PATH, THRESHOLD_MICROS);
    let payload = b"4";
    let msg = Message::from_slice(payload);
    let result = wx_storage::process_msg(&msg, &store);
    assert!(result.is_err())
}

#[test]
fn put_and_get_should_work() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, THRESHOLD_MICROS);
    
    // put
    let key = "test";
    let value = "testval".as_bytes();
    let kv = serialize(&(key, value)).unwrap();
    let mut payload = [0u8].to_vec();
    payload.extend_from_slice(&kv);
    let msg = Message::from_slice(&payload);
    let result = wx_storage::process_msg(&msg, &store);
    assert!(result.is_ok());

    // get
    let mut payload = [1u8].to_vec();
    payload.extend_from_slice(key.as_bytes());
    let msg = Message::from_slice(&payload);
    let result = wx_storage::process_msg(&msg, &store);
    assert!(result.is_ok());
    assert!(result.unwrap() == value);
}

#[test]
fn get_should_return_nothing_if_key_not_found() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, THRESHOLD_MICROS);
    let key = "i-do-not-exist";
    let mut payload = [1u8].to_vec();
    payload.extend_from_slice(key.as_bytes());
    let msg = Message::from_slice(&payload);
    let result = wx_storage::process_msg(&msg, &store);
    let expected: Vec<u8> = vec![];
    assert!(result.unwrap() == expected);
}

#[test]
fn put_event_should_return_a_u64() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, THRESHOLD_MICROS);

    let event = EventMessage {
        ingest_ts: 0,
        event_ts: 1548378900711570,
        event_type: "a",
        data: "b".to_string(),
    };

    let mut payload = [2u8].to_vec();
    payload.extend_from_slice(&serialize(&event).unwrap());
    let msg = Message::from_slice(&payload);
    let result = wx_storage::process_msg(&msg, &store);
    assert!(result.unwrap().len() == 8)
}

#[test]
fn get_event_should_return_events_newer_than_threshold() {
    destroy_store();
    let short_threshold_micros = 1000 * 1000; // 1s
    let sleep_duration = time::Duration::from_secs(1);
    let store = Store::new(TEST_STORE_PATH, short_threshold_micros);

    let event = EventMessage {
        ingest_ts: 0,
        event_ts: 1548378900711570,
        event_type: "abc",
        data: "this is a fake bulletin".to_string(),
    };

    let mut payload = [2u8].to_vec();
    payload.extend_from_slice(&serialize(&event).unwrap());
    let msg = Message::from_slice(&payload);

    // put 1 "old" message and sleep
    wx_storage::process_msg(&msg, &store).unwrap();
    thread::sleep(sleep_duration);

    // put 1 "new" messages and capture the ingest_ts
    let value = wx_storage::process_msg(&msg, &store).unwrap();
    let expected: u64 = deserialize(&value).unwrap();

    let payload = [3u8].to_vec();
    let msg = Message::from_slice(&payload);
    let result = wx_storage::process_msg(&msg, &store).unwrap();
    let result: Vec<EventMessage> = deserialize(&result).unwrap();
    assert!(result.len() == 1);
    assert!(result[0].ingest_ts == expected);
}

#[test]
fn get_events_should_seek_correctly() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, THRESHOLD_MICROS);

    let event = EventMessage {
        ingest_ts: 0,
        event_ts: 1548378900711570,
        event_type: "abc",
        data: "this is a fake bulletin".to_string(),
    };

    let mut payload = [2u8].to_vec();
    payload.extend_from_slice(&serialize(&event).unwrap());
    let msg = Message::from_slice(&payload);

    // put 1 "old" message
    wx_storage::process_msg(&msg, &store).unwrap();

    // put 1 message and capture the ingest_ts
    let value = wx_storage::process_msg(&msg, &store).unwrap();

    // put 1 "new" message
    let expected = wx_storage::process_msg(&msg, &store).unwrap();
    let expected: u64 = deserialize(&expected).unwrap();

    // get events, passing the last seen ts
    let mut payload = [3u8].to_vec();
    payload.extend_from_slice(&value);

    let msg = Message::from_slice(&payload);
    let result = wx_storage::process_msg(&msg, &store).unwrap();
    let result: Vec<EventMessage> = deserialize(&result).unwrap();
    assert!(result.len() == 1);
    assert!(result[0].ingest_ts == expected);
}

fn destroy_store() {
    let opts = Options::default();
    DB::destroy(&opts, TEST_STORE_PATH).unwrap();
}
