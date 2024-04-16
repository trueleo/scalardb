use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScalarValue {
    String(String),
    Number(i64),
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::String(x) => f.write_str(&x),
            ScalarValue::Number(x) => write!(f, "{}", x),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum DataType {
    String(usize),
    Number,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    pub feilds: Vec<(String, DataType)>,
}

impl Schema {
    pub fn row_size(&self) -> usize {
        self.feilds
            .iter()
            .map(|(_, x)| match x {
                DataType::String(size) => *size,
                DataType::Number => 8,
            })
            .sum()
    }
}
