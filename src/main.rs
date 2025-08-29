pub mod lib;

use std::env;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};


// Bring your types/constants into scope
// (If your code is in a module, adjust the paths accordingly)
use lib::{
    Bus,
    NUM_TOPICS, BITWORDS, NUM_EXCHANGES, NUM_MARKETS, NUM_SYMBOLS, NUM_CHANNELS,
};

/// Compute a flat topic_id from (exchange, market, symbol, channel)
#[inline]
fn topic_id(exchange: usize, market: usize, symbol: usize, channel: usize) -> usize {
    debug_assert!(exchange < NUM_EXCHANGES);
    debug_assert!(market < NUM_MARKETS);
    debug_assert!(symbol < NUM_SYMBOLS);
    debug_assert!(channel < NUM_CHANNELS);

    ((exchange * NUM_MARKETS + market) * NUM_SYMBOLS + symbol) * NUM_CHANNELS + channel
}

/// Set a single bit in the subscription mask
#[inline]
fn set_bit(mask: &mut [u64; BITWORDS], topic_id: usize) {
    debug_assert!(topic_id < NUM_TOPICS);
    let w = topic_id / 64;
    let b = topic_id % 64;
    mask[w] |= 1u64 << b;
}

/// Parse a comma-separated list like "1,2,3" (ignores whitespace)
fn parse_topic_list(s: &str) -> Vec<usize> {
    s.split(',')
        .filter_map(|t| t.trim().parse::<usize>().ok())
        .collect()
}

/// Current timestamp (ms) just for demo payloads
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Producer loop: publishes payloads to a single topic at a fixed rate
fn run_producer(bus_name: &str, topic_id: usize, rate_hz: u64) {
    let bus = Bus::open_or_create(bus_name);

    // Start after the latest observed sequence for this topic to avoid rewinding
    let mut seq = bus.get_latest_seq(topic_id).saturating_add(1);

    let interval = if rate_hz == 0 { Duration::from_millis(100) }
                   else { Duration::from_millis(1000 / rate_hz) };

    println!(
        "[producer] bus='{}' topic_id={} starting seq={} rate={} msg/s",
        bus_name, topic_id, seq, rate_hz
    );

    loop {
        let payload = format!("hello from producer: seq={} ts={}ms", seq, now_ms());
        bus.write(topic_id, seq, payload.as_bytes());
        bus.notify(topic_id);

        // For demo: print what we just wrote
        println!("[producer] wrote topic={} seq={} '{}'", topic_id, seq, payload);

        seq = seq.wrapping_add(1);
        thread::sleep(interval);
    }
}

/// Consumer loop:
///  - subscribes to a set of topic_ids via mask
///  - blocks on semaphore
///  - drains pending topics and prints the latest message per topic
fn run_consumer(bus_name: &str, subscribed_topics: &[usize]) {
    let bus = Bus::open_or_create(bus_name);

    // Build subscription mask
    let mut mask = [0u64; BITWORDS];
    if subscribed_topics.is_empty() {
        // If nothing specified, subscribe to ALL topics
        for t in 0..NUM_TOPICS {
            set_bit(&mut mask, t);
        }
        println!("[consumer] Subscribed to ALL topics ({} total)", NUM_TOPICS);
    } else {
        for &t in subscribed_topics {
            if t < NUM_TOPICS {
                set_bit(&mut mask, t);
            } else {
                eprintln!("[consumer] WARNING: ignoring out-of-range topic_id {}", t);
            }
        }
        println!("[consumer] Subscribed to topics: {:?}", subscribed_topics);
    }

    // Keep last-seen sequence per topic to demonstrate catching up
    let mut last_seen = vec![0u64; NUM_TOPICS];

    // Drain handler: invoked for each pending topic bit
    let mut on_topic = |t: usize| {
        // Strategy 1 (simple demo): read only the latest message for that topic.
        // Strategy 2 (uncomment) to walk from last_seen+1 .. latest (if you don't want to skip).
        let latest = bus.get_latest_seq(t);

        if latest == 0 {
            // Probably empty (or valid message with seq=0). For demo, just attempt to read latest.
        }

        // Attempt to read the latest message
        if let Some(msg) = bus.read(t, latest) {
            // Safety: get_text() uses from_utf8_unchecked, so ensure we wrote utf8 payloads.
            println!(
                "[consumer] topic={} seq={} len={} '{}'",
                t,
                msg.seq,
                msg.data_len,
                msg.get_text()
            );
            last_seen[t] = msg.seq;
        } else {
            // If the slot was overwritten or not matching (race), you can scan backwards/forwards
            // or fall back to scanning the ring. For demo we just note it.
            // You could also iterate from last_seen[t]+1..=latest and read each if present.
            // Example (optional):
            /*
            let mut s = last_seen[t].saturating_add(1);
            while s <= latest {
                if let Some(m) = bus.read(t, s) {
                    println!("[consumer] topic={} seq={} '{}'", t, m.seq, m.get_text());
                    last_seen[t] = m.seq;
                }
                s += 1;
            }
            */
        }
    };

    println!("[consumer] waiting for notifications on bus='{}' ...", bus_name);

    // Block until at least one topic is pending; drain until the semaphore is empty
    loop {
        bus.wait_and_drain_mask(&mask, &mut on_topic);
    }
}

fn print_usage(program: &str) {
    eprintln!(
        r#"Usage:
  {prog} producer <bus_name> <topic_id> [rate_hz]
  {prog} consumer <bus_name> [topic_list]

Examples:
  # Producer writes to topic 42 at 4 msgs/sec
  {prog} producer mybus 42 4

  # Consumer subscribes to topics 41,42,43
  {prog} consumer mybus 41,42,43

  # Consumer subscribes to ALL topics (omit topic_list)
  {prog} consumer mybus
"#,
        prog = program
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        print_usage(&args[0]);
        return;
    }

    let role = args[1].as_str();
    match role {
        "producer" => {
            if args.len() < 4 {
                print_usage(&args[0]);
                return;
            }
            let bus_name = &args[2];
            let topic_id: usize = args[3].parse().expect("topic_id must be usize");
            let rate_hz: u64 = if args.len() >= 5 {
                args[4].parse().unwrap_or(4)
            } else {
                4
            };
            run_producer(bus_name, topic_id, rate_hz);
        }
        "consumer" => {
            let bus_name = &args[2];
            let topics: Vec<usize> = if args.len() >= 4 {
                parse_topic_list(&args[3])
            } else {
                vec![]
            };
            run_consumer(bus_name, &topics);
        }
        _ => {
            print_usage(&args[0]);
        }
    }
}
