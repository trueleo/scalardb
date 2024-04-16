use std::io::Write;

pub struct Repl {
    history: Vec<String>,
}

impl Repl {
    pub fn new() -> Self {
        Self {
            history: Vec::default(),
        }
    }

    pub fn init(&self) {
        println!("{}", welcome());
    }

    // Returns None on exit
    pub fn input(&mut self) -> Option<String> {
        print!("sqlite> ");
        std::io::stdout().flush().expect("Failed to flush");
        let mut line = String::new();
        let read_bytes = std::io::stdin()
            .read_line(&mut line)
            .expect("Error reading from stdin");
        if line.ends_with('\n') {
            line.truncate(line.len() - 1)
        }
        if read_bytes == 0 {
            std::process::exit(0)
        }
        self.history.push(line.clone());
        Some(line)
    }
}

fn welcome() -> String {
    "Welcome to Sqlite".to_string()
}

