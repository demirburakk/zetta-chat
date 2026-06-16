# Zetta-Chat

A terminal-based chat client and server application designed to demonstrate the capabilities of the [zetta-transport](https://github.com/demirburakk/zetta-transport) protocol.

## Features

- **TUI (Terminal User Interface):** Beautiful, modern terminal UI built with `ratatui`.
- **Fault-Tolerant Networking:** Automatically reconnects upon network failure, keeps connections alive through advanced firewall/NAT systems.
- **Robust Input Handling:** Unicode support built-in; typing long messages or using emojis will not break the UI.

## Getting Started

### Prerequisites

Ensure you have Rust installed.

### Installation

Clone the repository and build the project using Cargo:

```bash
git clone https://github.com/demirburakk/zetta-chat.git
cd zetta-chat
cargo build --release
```

### Running the Server

Start the chat server to accept incoming connections:

```bash
cargo run --release --bin server
```
By default, the server binds to `0.0.0.0:8080`.

### Running the Client

Start the TUI chat client in another terminal instance:

```bash
cargo run --release --bin zetta-chat
```
If you wish to connect to a server hosted elsewhere (like Azure or a remote VPS), pass the IP and Port as an argument:

```bash
cargo run --release --bin zetta-chat 20.204.47.105:8080
```

## Architecture

This application acts as a testbed for the custom `zetta-transport` underlying protocol. The logic is separated into an async networking loop fetching messages with `tokio` and a drawing thread rendering the chat interface to the terminal.

## License

This project is licensed under the MIT License.
