use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration}; // Zamanlayıcıyı ekledik
use tracing::{error, info, warn};
use zetta_transport::transport::endpoint::ZtEndpoint;

#[derive(Debug)]
pub enum UiCommand {
    SendMessage(String),
    Quit,
}

#[derive(Debug)]
pub enum NetEvent {
    StatusChanged(String),
    MessageReceived(String),
    Error(String),
}

pub async fn run_network_task(
    server_addr_str: &str,
    cc_algo: zetta_transport::transport::CongestionControlAlgorithm,
    mut ui_rx: mpsc::Receiver<UiCommand>,
    net_tx: mpsc::Sender<NetEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    let target_addr: SocketAddr = server_addr_str.parse()?;

    loop {
        let _ = net_tx.send(NetEvent::StatusChanged("Binding local endpoint...".into())).await;

        let client = match ZtEndpoint::bind_with_config("0.0.0.0:0", None, cc_algo).await {
            Ok(ep) => ep,
            Err(e) => {
                let msg = format!("Bind failed: {:?}", e);
                error!("{}", msg);
                let _ = net_tx.send(NetEvent::Error(msg)).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        let _ = net_tx.send(NetEvent::StatusChanged(format!("Connecting to {}...", target_addr))).await;
        info!("Initiating connection to Azure server at {}", target_addr);

        let conn = match client.connect(target_addr).await {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("Connection failed: {:?}", e);
                error!("{}", msg);
                let _ = net_tx.send(NetEvent::Error(msg)).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        let _ = net_tx.send(NetEvent::StatusChanged(format!("Connected to {}", target_addr))).await;
        info!("Successfully connected to Azure server.");

        let mut stream = match conn.open_stream().await {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("Failed to open stream: {:?}", e);
                error!("{}", msg);
                let _ = net_tx.send(NetEvent::Error(msg)).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        // KEEP-ALIVE ZAMANLAYICISI: Her 15 saniyede bir tetiklenecek
        let mut keepalive_interval = interval(Duration::from_secs(15));
        let mut quit_received = false;
        let mut buf = [0u8; 4096];

        loop {
            tokio::select! {
                incoming = stream.read(&mut buf) => {
                    match incoming {
                        Ok(n) => {
                            if n == 0 {
                                warn!("Stream closed by the remote server. Reconnecting...");
                                let _ = net_tx.send(NetEvent::StatusChanged("Disconnected (Stream closed). Reconnecting...".into())).await;
                                break; // İç döngüden çık, dış döngü tekrar bağlanacak
                            }
                            let bytes = &buf[..n];
                            if bytes != b"\0" {
                                let text = String::from_utf8_lossy(bytes).to_string();
                                let _ = net_tx.send(NetEvent::MessageReceived(text)).await;
                            }
                        }
                        Err(e) => {
                            warn!("Stream read error: {:?}. Reconnecting...", e);
                            let _ = net_tx.send(NetEvent::StatusChanged("Disconnected (Read error). Reconnecting...".into())).await;
                            break;
                        }
                    }
                }

                ui_cmd = ui_rx.recv() => {
                    match ui_cmd {
                        Some(UiCommand::SendMessage(msg)) => {
                            let send_res = async {
                                stream.write_all(msg.as_bytes()).await?;
                                stream.flush().await
                            }.await;
                            if let Err(e) = send_res {
                                error!("Failed to send data: {:?}", e);
                                let _ = net_tx.send(NetEvent::Error(format!("Send failed: {:?}", e))).await;
                            } else {
                                info!("Payload sent to server reliably.");
                            }
                        }
                        Some(UiCommand::Quit) => {
                            info!("Quit command received, closing stream and connection.");
                            let _ = stream.shutdown().await;
                            let _ = conn.close().await;
                            quit_received = true;
                            break;
                        }
                        None => {
                            quit_received = true;
                            break;
                        }
                    }
                }
                
                // KEEP-ALIVE GÖNDERİMİ: Azure Firewall'u ve Sunucuyu uyanık tutar
                _ = keepalive_interval.tick() => {
                    let keepalive_res = async {
                        stream.write_all(b"\0").await?;
                        stream.flush().await
                    }.await;
                    if let Err(e) = keepalive_res {
                         warn!("Failed to send keep-alive: {:?}. Reconnecting...", e);
                         break; // Bağlantı koptu, yeniden bağlan
                    }
                }
            }
        }

        if quit_received {
            break;
        }
        
        // Yeniden bağlanmadan önce biraz bekle
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}