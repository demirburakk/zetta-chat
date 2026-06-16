use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

    // DÜZELTME: Herkese açık olması için 0.0.0.0 yapıldı
    let addr = "0.0.0.0:8080";
    println!("Starting ZettaTransport Chat Server on {}...", addr);

    let server = ZtEndpoint::bind(addr, None).await?;

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
                    loop {
                        tokio::select! {
                            // DÜZELTME 1: Burası stream.recv() olmalı! (Ağdan o anki kullanıcıyı dinler)
                            result = stream.recv() => {
                                match result {
                                    Some(data) => {
                                        // DÜZELTME 2: Keep-alive (boş veri) pinglerini chate gönderme
                                        if !data.is_empty() && data.as_ref() != b"\0"{
                                            let msg = String::from_utf8_lossy(&data).to_string();
                                            println!("User {}: {}", my_id, msg);
                                            
                                            let _ = tx_inner.send(ChatMessage {
                                                sender_id: my_id,
                                                content: msg,
                                            });
                                        }
                                    }
                                    None => {
                                        println!("User {} disconnected.", my_id);
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
                                            
                                            let _ = stream.send(formatted_msg.as_bytes()).await;
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