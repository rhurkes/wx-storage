#[macro_use]
extern crate slog;

use bincode::serialize;
use wx::store::Status;
use wx::util::Logger;
use wx_store::Store;
use zmq::{Context, Message};

const APP_NAME: &str = "wx_store";
const STORE_PATH: &str = "wx_store";
const EVENT_THRESHOLD_MICROS: u64 = 1000 * 1000 * 60 * 60; // 1 hr
const FETCH_FAILURE_THRESHOLD_MICROS: u64 = 1000 * 1000 * 60 * 3; // 3 minutes
const ZMQ_ADDRESS: &str = "tcp://127.0.0.1:31337";

fn main() {
    let ctx = Context::new();
    let sock = ctx.socket(zmq::REP).unwrap();
    let logger = Logger::new(APP_NAME);
    let store = Store::new(
        STORE_PATH,
        EVENT_THRESHOLD_MICROS,
        FETCH_FAILURE_THRESHOLD_MICROS,
    );
    let mut msg = Message::new();

    info!(logger, "initializing"; "zmq_address" => ZMQ_ADDRESS, "store_path" => STORE_PATH);
    sock.bind(ZMQ_ADDRESS).unwrap();

    loop {
        if sock.recv(&mut msg, 0).is_ok() {
            match wx_store::process_msg(&msg, &store) {
                Ok(value) => {
                    let mut payload = [Status::OkByte.value()].to_vec();
                    payload.extend_from_slice(&value);
                    sock.send(payload, 0).unwrap();
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    error!(logger, "listener"; "msg" => &error_msg);
                    let mut payload = [Status::ErrorByte.value()].to_vec();
                    let error_bytes = serialize(&error_msg).unwrap();
                    payload.extend_from_slice(&error_bytes);
                    sock.send(payload, 0).unwrap();
                }
            }
        } else {
            error!(logger, "listener"; "msg" => "error receiving on socket");
        }
    }
}
