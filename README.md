# IPCBus – Shared Memory Pub/Sub Bus in Rust

This project implements a **high-performance inter-process communication (IPC) bus** using:

- **Shared memory** for zero-copy message passing.
- **POSIX named semaphores** for efficient notifications.
- A **bitmap-based pending mask** for fast topic filtering.

It’s designed for scenarios where multiple producers and consumers need to exchange messages across processes with minimal latency.

---

## ✨ Features

- Fixed-size **ring buffer per topic** for predictable memory usage.
- **Atomic bitmap** to track pending topics.
- **Semaphore-based wake-up** (no busy-waiting).
- Supports **multiple processes** and **multiple subscribers**.
- Works on **Linux** and **macOS**.

---

## 🛠 How It Works

- **Producer**:
  1. Writes a message into the shared memory ring for a given topic.
  2. Marks the topic as pending in the bitmap.
  3. Posts the semaphore to wake up consumers.

- **Consumer**:
  1. Waits on the semaphore (blocks until notified).
  2. Reads the pending bitmap and processes topics it subscribed to.
  3. Clears the bits after processing.

---

## 📦 Build

```bash
cargo build --release
```

## 🚀 Deploy
```bash
cargo run --release -- producer mybus 42 4
cargo run --release -- consumer mybus 41,42,43
```