use crate::errors::Error;

pub fn do_meta_commands(command: Command) -> Result<(), Error> {
    match command {
        Command::Exit => std::process::exit(0),
    }
}

pub enum Command {
    Exit,
}

impl std::str::FromStr for Command {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with('.') {
            return Err(Error::UnrecognizedCommand);
        }

        let command = match &s[1..] {
            "exit" => Command::Exit,
            _ => return Err(Error::UnrecognizedCommand),
        };

        Ok(command)
    }
}
