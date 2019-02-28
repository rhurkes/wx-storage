extern crate wx_store;

use bincode::{deserialize, serialize};
use rocksdb::{Options, DB};
use std::{str, thread, time};
use wx::domain::{Event, EventType, FetchFailure, WxApp};
use wx_store::Store;
use zmq::Message;

const EVENT_THRESHOLD_MICROS: u64 = 1000 * 1000 * 60 * 60; // 1 hr
const FETCH_FAILURE_THRESHOLD_MICROS: u64 = 1000 * 1000 * 60 * 3;   // 3 minutes
const TEST_STORE_PATH: &str = "wx_test";

fn destroy_store() {
    let opts = Options::default();
    DB::destroy(&opts, TEST_STORE_PATH).unwrap();
}

fn get_test_event() -> Event {
    Event {
        event_ts: 1548378900711570,
        event_type: EventType::NwsLsr,
        expires_ts: None,
        fetch_status: None,
        image_uri: None,
        ingest_ts: 0,
        location: None,
        md: None,
        outlook: None,
        report: None,
        summary: String::from("summary"),
        text: None,
        title: String::from("title"),
        valid_ts: None,
        warning: None,
        watch: None,
    }
}

#[test]
fn zero_message_length_should_error() {
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);
    let msg = Message::new();
    let result = wx_store::process_msg(&msg, &store);
    assert!(result.is_err())
}

#[test]
fn unknown_command_should_error() {
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);
    let payload = b"4";
    let msg = Message::from_slice(payload);
    let result = wx_store::process_msg(&msg, &store);
    assert!(result.is_err())
}

#[test]
fn put_and_get_should_work() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);

    // put
    let key = "test";
    let value = "testval".as_bytes();
    let kv = serialize(&(key, value)).unwrap();
    let mut payload = [0u8].to_vec();
    payload.extend_from_slice(&kv);
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store);
    assert!(result.is_ok());

    // get
    let mut payload = [1u8].to_vec();
    payload.extend_from_slice(key.as_bytes());
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store);
    assert!(result.is_ok());
    assert!(result.unwrap() == value);
}

#[test]
fn get_should_return_nothing_if_key_not_found() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);
    let key = "i-do-not-exist";
    let mut payload = [1u8].to_vec();
    payload.extend_from_slice(key.as_bytes());
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store);
    let expected: Vec<u8> = vec![];
    assert!(result.unwrap() == expected);
}

#[test]
fn put_event_should_return_a_u64() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);
    let event = get_test_event();
    let mut payload = [2u8].to_vec();
    payload.extend_from_slice(&serialize(&event).unwrap());
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store);
    assert!(result.unwrap().len() == 8)
}

#[test]
fn put_event_and_get_events_should_persist_an_event_and_populate_ingest_ts() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);
    let event = get_test_event();

    let mut payload = [2u8].to_vec();
    payload.extend_from_slice(&serialize(&event).unwrap());
    let msg = Message::from_slice(&payload);
    let key = wx_store::process_msg(&msg, &store).unwrap();
    let expected_key: u64 = deserialize(&key).unwrap();

    let payload = [3u8].to_vec();
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store).unwrap();
    let result: Vec<Event> = deserialize(&result).unwrap();
    let result = &result[0];
    assert_eq!(result.event_ts, event.event_ts);
    assert_eq!(result.expires_ts, event.expires_ts);
    assert_eq!(result.ingest_ts, expected_key);
    assert_eq!(result.event_type, event.event_type);
}

#[test]
fn get_event_should_return_events_newer_than_threshold() {
    destroy_store();
    let event_threshold_micros = 1000 * 1000; // 1s
    let sleep_duration = time::Duration::from_secs(1);
    let store = Store::new(TEST_STORE_PATH, event_threshold_micros, FETCH_FAILURE_THRESHOLD_MICROS);

    let event = get_test_event();

    let mut payload = [2u8].to_vec();
    payload.extend_from_slice(&serialize(&event).unwrap());
    let msg = Message::from_slice(&payload);

    // put 1 "old" message and sleep
    wx_store::process_msg(&msg, &store).unwrap();
    thread::sleep(sleep_duration);

    // put 1 "new" messages and capture the ingest_ts
    let value = wx_store::process_msg(&msg, &store).unwrap();
    let expected: u64 = deserialize(&value).unwrap();

    let payload = [3u8].to_vec();
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store).unwrap();
    let result: Vec<Event> = deserialize(&result).unwrap();
    assert!(result.len() == 1);
    assert!(result[0].ingest_ts == expected);
}

#[test]
fn get_events_should_seek_correctly() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);
    let event = get_test_event();

    let mut payload = [2u8].to_vec();
    payload.extend_from_slice(&serialize(&event).unwrap());
    let msg = Message::from_slice(&payload);

    // put 1 "old" message
    wx_store::process_msg(&msg, &store).unwrap();

    // put 1 message and capture the ingest_ts
    let value: u64 = deserialize(&wx_store::process_msg(&msg, &store).unwrap()).unwrap();

    // put 1 "new" message
    let expected = wx_store::process_msg(&msg, &store).unwrap();
    let expected: u64 = deserialize(&expected).unwrap();

    // get events, passing the last seen ts
    let mut payload = [3u8].to_vec();
    let ingest_string_bytes = serialize(&value.to_string()).unwrap();
    payload.extend_from_slice(&ingest_string_bytes);

    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store).unwrap();
    let result: Vec<Event> = deserialize(&result).unwrap();
    assert!(result.len() == 1);
    assert!(result[0].ingest_ts == expected);
}

#[test]
fn get_events_handles_zero_events() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);
    let mut payload = [3u8].to_vec();
    let value: Vec<u8> = vec![];
    payload.extend_from_slice(&value);
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store).unwrap();
    let result: Vec<Event> = deserialize(&result).unwrap();
    assert!(result.len() == 0);
}

#[test]
fn put_fetch_failure_should_persist_failures() {
    destroy_store();
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, FETCH_FAILURE_THRESHOLD_MICROS);

    // put failure
    let failure = FetchFailure{app: WxApp::Admin, ingest_ts: 0};
    let mut payload = [5u8].to_vec();
    payload.extend_from_slice(&serialize(&failure).unwrap());
    let msg = Message::from_slice(&payload);
    wx_store::process_msg(&msg, &store).unwrap();

    // get failures
    let payload = [6u8].to_vec();
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store).unwrap();
    let result: Vec<FetchFailure> = deserialize(&result).unwrap();

    assert!(result.len() == 1);
    assert!(result[0].ingest_ts > 0);
}

#[test]
fn get_fetch_failures_should_return_recent_failures() {
    destroy_store();
    let fetch_failure_threshold_micros = 1000 * 1000; // 1s
    let sleep_duration = time::Duration::from_secs(1);
    let store = Store::new(TEST_STORE_PATH, EVENT_THRESHOLD_MICROS, fetch_failure_threshold_micros);

    // put "old" failure
    let failure = FetchFailure{app: WxApp::Admin, ingest_ts: 0};
    let mut payload = [5u8].to_vec();
    payload.extend_from_slice(&serialize(&failure).unwrap());
    let msg = Message::from_slice(&payload);
    wx_store::process_msg(&msg, &store).unwrap();

    thread::sleep(sleep_duration);

    // put "new" failure
    let failure = FetchFailure{app: WxApp::Admin, ingest_ts: 0};
    let mut payload = [5u8].to_vec();
    payload.extend_from_slice(&serialize(&failure).unwrap());
    let msg = Message::from_slice(&payload);
    wx_store::process_msg(&msg, &store).unwrap();

    // get failures
    let payload = [6u8].to_vec();
    let msg = Message::from_slice(&payload);
    let result = wx_store::process_msg(&msg, &store).unwrap();
    let result: Vec<FetchFailure> = deserialize(&result).unwrap();

    assert!(result.len() == 1);
    assert!(result[0].ingest_ts > 0);
}
