use std::{io::Write, mem};

use crate::datatype::{DataType, ScalarValue, Schema};

const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = mem::size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

#[derive(Debug, PartialEq, Eq)]
pub enum Pos {
    Left,
    Right,
}

pub struct LeafNode {
    value_size: usize,
    bytes: Box<[u8; 4096]>,
}

impl LeafNode {
    const NODE_NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
    const NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
    const NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + Self::NODE_NUM_CELLS_SIZE;

    const NODE_KEY_SIZE: usize = mem::size_of::<u32>();
    const NODE_SPACE_FOR_CELLS: usize = 4096 - Self::NODE_HEADER_SIZE;

    pub fn cell_size(&self) -> usize {
        Self::NODE_KEY_SIZE + self.value_size
    }

    pub fn max_cells(&self) -> usize {
        (4096 - Self::NODE_HEADER_SIZE) / self.cell_size()
    }

    pub fn num_cells(&self) -> u32 {
        u32::from_ne_bytes(
            self.bytes[Self::NODE_NUM_CELLS_OFFSET..Self::NODE_NUM_CELLS_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    pub fn read_row(&self, index: usize, schema: &Schema) -> Vec<ScalarValue> {
        let mut offset = Self::NODE_HEADER_SIZE + index * self.cell_size();

        let key = &self.bytes[offset..Self::NODE_KEY_SIZE];
        let key = u32::from_ne_bytes(key.try_into().unwrap());

        let values_bytes = &self.bytes[(offset + Self::NODE_KEY_SIZE)..self.value_size];
        let mut value_offset = 0;
        let mut values = Vec::new();

        for (_, ty) in &schema.feilds {
            let value = match ty {
                DataType::String(size) => {
                    let len = values_bytes[value_offset] as usize;
                    if len != 0 {
                        let bytes = &values_bytes[(value_offset + 1)..=(value_offset + len)];
                        value_offset += size;
                        let string = String::from_utf8(bytes.to_owned()).unwrap();
                        ScalarValue::String(string)
                    } else {
                        ScalarValue::String("".to_string())
                    }
                }
                DataType::Number => {
                    let bytes = &values_bytes[value_offset..value_offset + 8];
                    value_offset += 8;
                    ScalarValue::Number(i64::from_ne_bytes(bytes.try_into().unwrap()))
                }
            };
            values.push(value);
        }
        values
    }

    pub fn write_row(&mut self, index: usize, schema: &Schema, key: u32, values: Vec<ScalarValue>) {
        let offset = Self::NODE_HEADER_SIZE + index * self.cell_size();
        let cell_size = self.cell_size();
        let cell = &mut self.bytes[offset..cell_size];
        cell[..Self::NODE_KEY_SIZE].copy_from_slice(&key.to_ne_bytes());
        let mut cell_offset = Self::NODE_KEY_SIZE;

        let mut values = values.into_iter();

        for (_, ty) in &schema.feilds {
            match ty {
                DataType::String(size) => {
                    let ScalarValue::String(value) = values.next().unwrap() else {
                        panic!()
                    };
                    let bytes = &mut cell[cell_offset..cell_offset + size];
                    bytes[0] = value.len() as u8;
                    (&mut bytes[1..]).write(value.as_bytes()).unwrap();
                    cell_offset += size
                }
                DataType::Number => {
                    let ScalarValue::Number(value) = values.next().unwrap() else {
                        panic!()
                    };
                    (&mut cell[cell_offset..])
                        .write(&value.to_ne_bytes())
                        .unwrap();
                    cell_offset += 8
                }
            };
        }
    }
}

pub struct InternalNode {
    bytes: Box<[u8; 4098]>,
}

impl InternalNode {
    const NODE_NUM_KEYS_SIZE: usize = mem::size_of::<u32>();
    const NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
    const NODE_RIGHT_CHILD_SIZE: usize = mem::size_of::<u32>();
    const NODE_RIGHT_CHILD_OFFSET: usize = Self::NODE_NUM_KEYS_OFFSET + Self::NODE_NUM_KEYS_SIZE;
    const NODE_HEADER_SIZE: usize =
        COMMON_NODE_HEADER_SIZE + Self::NODE_NUM_KEYS_SIZE + Self::NODE_RIGHT_CHILD_SIZE;
    const NODE_KEY_SIZE: usize = mem::size_of::<u32>();
    const NODE_CHILD_SIZE: usize = mem::size_of::<u32>();
    const NODE_CELL_SIZE: usize = Self::NODE_CHILD_SIZE + Self::NODE_KEY_SIZE;
    const NODE_MAX_CELLS: usize = (4096 - Self::NODE_HEADER_SIZE) / Self::NODE_CELL_SIZE;

    pub fn set_root_node(&mut self) {
        self.bytes[IS_ROOT_OFFSET] = 1u8;
    }

    pub fn root_node(&self) -> bool {
        self.bytes[IS_ROOT_OFFSET] != 0
    }

    pub fn num_keys(&self) -> u32 {
        u32::from_ne_bytes(
            self.bytes[Self::NODE_NUM_KEYS_OFFSET..Self::NODE_NUM_KEYS_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    #[inline]
    pub fn max_cells(&self) -> usize {
        Self::NODE_MAX_CELLS
    }

    #[inline]
    fn key_offset(index: usize) -> usize {
        Self::cell_offset(index) + Self::NODE_CHILD_SIZE
    }

    #[inline]
    fn cell_offset(index: usize) -> usize {
        Self::NODE_HEADER_SIZE + index * Self::NODE_CELL_SIZE
    }

    pub fn right_most_child(&self) -> u32 {
        let bytes = self.bytes[Self::NODE_RIGHT_CHILD_OFFSET..Self::NODE_RIGHT_CHILD_SIZE]
            .try_into()
            .unwrap();
        u32::from_ne_bytes(bytes)
    }

    pub fn key(&self, index: usize) -> u32 {
        let offset = Self::key_offset(index);
        let key_bytes = self.bytes[offset..offset + Self::NODE_KEY_SIZE]
            .try_into()
            .unwrap();
        u32::from_ne_bytes(key_bytes)
    }

    pub fn children(&self, mut index: usize, pos: Pos) -> u32 {
        let num_keys = self.num_keys();

        if pos == Pos::Right {
            index += 1
        }
        if index == num_keys as usize {
            return self.right_most_child();
        }
        if index > num_keys as usize {
            panic!("Oooo");
        }

        let offset = Self::cell_offset(index);
        let children_pointer_bytes = self.bytes[offset..Self::NODE_CHILD_SIZE]
            .try_into()
            .unwrap();
        u32::from_ne_bytes(children_pointer_bytes)
    }
}
