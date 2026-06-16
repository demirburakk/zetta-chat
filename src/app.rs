use crate::network::{NetEvent, UiCommand};
use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame,
};
use tokio::sync::mpsc;
use tracing::info;
use unicode_width::UnicodeWidthStr;

pub struct App {
    pub input: String,
    pub messages: Vec<String>,
    pub status: String,
    ui_tx: mpsc::Sender<UiCommand>,
}

impl App {
    pub fn new(ui_tx: mpsc::Sender<UiCommand>) -> Self {
        Self {
            input: String::new(),
            messages: Vec::new(),
            status: "Initializing...".to_string(),
            ui_tx,
        }
    }

    pub fn handle_net_event(&mut self, event: NetEvent) {
        match event {
            NetEvent::StatusChanged(new_status) => {
                info!("Status updated to: {}", new_status);
                self.status = new_status;
            }
            NetEvent::MessageReceived(msg) => {
                info!("Received message: {}", msg);
                self.messages.push(msg);
            }
            NetEvent::Error(err) => {
                self.messages.push(format!("ERROR: {}", err));
            }
        }
    }

    pub fn submit_message(&mut self) {
        if !self.input.is_empty() {
            let msg = self.input.clone();
            self.messages.push(format!("You: {}", msg));
            
            // Log user action safely
            info!("Sending message to server: {}", msg);
            
            let _ = self.ui_tx.try_send(UiCommand::SendMessage(msg));
            self.input.clear();
        }
    }

    pub fn send_quit(&self) {
        let _ = self.ui_tx.try_send(UiCommand::Quit);
    }

    pub fn draw(&self, f: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(2)
            .constraints(
                [
                    Constraint::Length(3), // Header / Status
                    Constraint::Min(1),    // Chat history
                    Constraint::Length(3), // Input box
                ]
                .as_ref(),
            )
            .split(f.size());

        // 1. Status Bar
        let status_style = if self.status.contains("Connected") {
            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Yellow)
        };
        let status_block = Paragraph::new(format!(" Status: {} ", self.status))
            .style(status_style)
            .block(Block::default().borders(Borders::ALL).title(" Zetta-Chat "));
        f.render_widget(status_block, chunks[0]);

        // 2. Chat Messages
        let messages: Vec<ListItem> = self
            .messages
            .iter()
            .map(|m| ListItem::new(m.clone()))
            .collect();
        let messages_list = List::new(messages).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Remote Stream "),
        );
        f.render_widget(messages_list, chunks[1]);

        // 3. Input Box
        let width = chunks[2].width.max(3) - 3; // Kenarlıklar ve imleç için pay bırak
        // Eğer input uzunsa metni kaydır (scroll)
        let scroll = self.input.width().saturating_sub(width as usize);
        
        let input_text = Paragraph::new(self.input.clone())
            .scroll((0, scroll as u16))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Input (Press Enter to Send, Esc to Quit) "),
            );
        f.render_widget(input_text, chunks[2]);
        
        // Cursor positioning
        f.set_cursor(
            chunks[2].x + ((self.input.width() - scroll) as u16) + 1,
            chunks[2].y + 1,
        );
    }
}