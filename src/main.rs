mod app;
mod network;

use app::App;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{error::Error, time::Duration};
use tokio::sync::mpsc;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 1. Setup Professional Logging to a file (prevents TUI corruption)
    let file_appender = tracing_appender::rolling::never(".", "zetta_chat.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("Starting Zetta-Chat Client...");

    // 2. Setup Terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // 3. Setup Channels for UI <-> Network Communication
    let (ui_tx, net_rx) = mpsc::channel(100);
    let (net_tx, ui_rx) = mpsc::channel(100);

    // 4. Create App State
    let mut app = App::new(ui_tx);

    // 5. Start Network Task (Azure Server details can be configured here)
    // Uygulama başlatılırken argüman verilirse onu kullanır, verilmezse varsayılan olarak lokal adresi kullanır
    let azure_server_addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:8080".to_string());
    
    tokio::spawn(async move {
        if let Err(e) = network::run_network_task(&azure_server_addr, net_rx, net_tx).await {
            error!("Network task terminated with error: {:?}", e);
        }
    });

    // 6. Run TUI Loop
    let res = run_app(&mut terminal, &mut app, ui_rx).await;

    // 7. Restore Terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        eprintln!("Application encountered an error: {err:?}");
        error!("Fatal UI Error: {:?}", err);
    }

    info!("Application shut down gracefully.");
    Ok(())
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
    mut ui_rx: mpsc::Receiver<network::NetEvent>,
) -> Result<(), Box<dyn Error>> {
    loop {
        terminal.draw(|f| app.draw(f))?;

        // Handle Network Events
        while let Ok(event) = ui_rx.try_recv() {
            app.handle_net_event(event);
        }

        // Handle Keyboard Events
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Esc => {
                        app.send_quit();
                        return Ok(());
                    }
                    KeyCode::Enter => app.submit_message(),
                    KeyCode::Char(c) => app.input.push(c),
                    KeyCode::Backspace => {
                        app.input.pop();
                    }
                    _ => {}
                }
            }
        }
    }
}