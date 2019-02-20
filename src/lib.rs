use bincode::{deserialize, serialize};
use rocksdb::{ColumnFamily, DBCompressionType, Options, DB};
use std::mem;
use wx::domain::Event;
use wx::error::{Error, WxError};
use wx::store::Command;
use zmq::Message;

const EVENTS_CF: &str = "events";

pub fn process_msg(msg: &Message, store: &Store) -> Result<Vec<u8>, Error> {
    if msg.len() == 0 {
        return Err(Error::Wx(<WxError>::new("invalid message length")));
    }

    let payload = &msg[1..];
    let command = Command::from(msg[0]);

    match command {
        Some(Command::Put) => store.put(payload),
        Some(Command::Get) => store.get(payload),
        Some(Command::PutEvent) => store.put_event(payload),
        Some(Command::GetEvents) => store.get_events(payload, false),
        Some(Command::GetAllEvents) => store.get_events(payload, true),
        _ => Err(Error::Wx(<WxError>::new("unknown command"))),
    }
}

pub struct Store {
    db: DB,
    events_cf: ColumnFamily,
    threshold_micros: u64,
}

impl Store {
    pub fn new(path: &str, threshold_micros: u64) -> Store {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(DBCompressionType::Lz4hc);

        let cfs = [EVENTS_CF];
        let db = DB::open_cf(&opts, path, &cfs).unwrap();
        let events_cf = db.cf_handle(EVENTS_CF).unwrap();

        Store {
            db,
            events_cf,
            threshold_micros,
        }
    }

    pub fn put(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let kv: (&str, &[u8]) = deserialize(payload).unwrap();
        let key = kv.0.as_bytes();
        self.db.put(&key, kv.1)?;

        Ok(key.to_vec())
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        match self.db.get(key)? {
            Some(value) => Ok(value.to_vec()),
            None => Ok(vec![]),
        }
    }

    /**
     * To get lexigraphical sorting to work the intended way, we use the bytes of a stringified
     * u64 as the key. This is an internal quirk that we don't want to expose, which is why we
     * return u64 bytes and not the actual key used.
     */
    pub fn put_event(&self, value: &[u8]) -> Result<Vec<u8>, Error> {
        let micros = wx::util::get_system_micros();
        let key = micros.to_string();
        let mut event: Event = deserialize(&value).unwrap();
        event.ingest_ts = micros;
        let value = serialize(&event).unwrap();
        self.db.put_cf(self.events_cf, &key.as_bytes(), &value)?;

        let micros_bytes = serialize(&micros).unwrap();

        Ok(micros_bytes)
    }

    pub fn get_events(&self, key: &[u8], get_all: bool) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        let mut count: u64 = 0;
        let mut iter = self.db.raw_iterator_cf(self.events_cf)?;

        if get_all {
            iter.seek_to_first()
        } else if key.is_empty() {
            let micros = wx::util::get_system_micros() - self.threshold_micros;
            let micros = micros.to_string();
            iter.seek(&micros.as_bytes());
        } else {
            // If the key is still valid and would be returned, then we need to skip it since
            // it has already been seen by the requester.
            let key: &str = deserialize(&key).unwrap(); // TODO this panics
            let key = key.parse::<u64>().unwrap();
            let key = key + 1;
            let key = key.to_string();
            iter.seek(&key.as_bytes());
        }

        while iter.valid() {
            let value = unsafe { iter.value_inner().unwrap() };
            buffer.extend_from_slice(&value);
            count += 1;
            iter.next();
        }

        let mut events_envelope = Vec::new();
        let count: [u8; 8] = unsafe { mem::transmute(count) };
        events_envelope.extend_from_slice(&count);
        events_envelope.extend_from_slice(&buffer);

        Ok(events_envelope)
    }
}
