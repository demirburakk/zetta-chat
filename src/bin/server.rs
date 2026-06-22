use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use zetta_transport::transport::endpoint::ZtEndpoint;

#[derive(Clone, Debug)]
struct ChatMessage {
    sender_id: usize,
    content: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Parse command line arguments: bind_addr and --cc <algorithm>
    let args: Vec<String> = std::env::args().collect();
    let mut addr = "0.0.0.0:8080".to_string();
    let mut cc_algo = zetta_transport::transport::CongestionControlAlgorithm::Cubic;

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--cc" && i + 1 < args.len() {
            match args[i + 1].to_lowercase().as_str() {
                "reno" => cc_algo = zetta_transport::transport::CongestionControlAlgorithm::Reno,
                "cubic" => cc_algo = zetta_transport::transport::CongestionControlAlgorithm::Cubic,
                other => {
                    eprintln!("Unknown congestion control algorithm: {}", other);
                    std::process::exit(1);
                }
            }
            i += 2;
        } else if args[i].starts_with('-') {
            eprintln!("Unknown option: {}", args[i]);
            std::process::exit(1);
        } else {
            addr = args[i].clone();
            i += 1;
        }
    }

    println!("Starting ZettaTransport Chat Server on {} with {:?} Congestion Control...", addr, cc_algo);

    let server = ZtEndpoint::bind_with_config(&addr, None, cc_algo).await?;

    let (tx, _rx) = broadcast::channel::<ChatMessage>(100);
    let user_counter = Arc::new(AtomicUsize::new(1));

    while let Some(mut conn) = server.accept().await {
        let my_id = user_counter.fetch_add(1, Ordering::SeqCst);
        println!("User {} connected!", my_id);

        let tx_clone = tx.clone();
        
        tokio::spawn(async move {
            while let Some(mut stream) = conn.accept_stream().await {
                println!("User {} opened a stream!", my_id);
                
                let mut rx = tx_clone.subscribe();
                let tx_inner = tx_clone.clone();

                let _ = tx_inner.send(ChatMessage {
                    sender_id: 0, 
                    content: format!("User {} joined the chat!", my_id),
                });

                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        tokio::select! {
                            result = stream.read(&mut buf) => {
                                match result {
                                    Ok(0) => {
                                        println!("User {} disconnected.", my_id);
                                        let _ = tx_inner.send(ChatMessage {
                                            sender_id: 0,
                                            content: format!("User {} left the chat.", my_id),
                                        });
                                        break;
                                    }
                                    Ok(n) => {
                                        let data = &buf[..n];
                                        if data != b"\0" {
                                            let msg = String::from_utf8_lossy(data).to_string();
                                            println!("User {}: {}", my_id, msg);
                                            
                                            let _ = tx_inner.send(ChatMessage {
                                                sender_id: my_id,
                                                content: msg,
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        println!("User {} read error: {:?}. Disconnecting.", my_id, e);
                                        let _ = tx_inner.send(ChatMessage {
                                            sender_id: 0,
                                            content: format!("User {} left the chat.", my_id),
                                        });
                                        break;
                                    }
                                }
                            }
                            
                            // Burası rx.recv() olarak kalmalı (Odadaki diğer kullanıcıların yayınını dinler)
                            result = rx.recv() => {
                                match result {
                                    Ok(chat_msg) => {
                                        if chat_msg.sender_id != my_id {
                                            let formatted_msg = if chat_msg.sender_id == 0 {
                                                format!("[System]: {}", chat_msg.content)
                                            } else {
                                                format!("User {}: {}", chat_msg.sender_id, chat_msg.content)
                                            };
                                            
                                            let send_res = async {
                                                stream.write_all(formatted_msg.as_bytes()).await?;
                                                stream.flush().await
                                            }.await;
                                            if let Err(e) = send_res {
                                                println!("Failed to send to user {}: {:?}", my_id, e);
                                                break;
                                            }
                                        }
                                    }
                                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                        continue; 
                                    }
                                    Err(_) => break, 
                                }
                            }
                        }
                    }
                });
            }
        });
    }

    Ok(())
}