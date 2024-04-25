use std::{io::Write, mem};

use crate::{
    datatype::{DataType, ScalarValue, Schema},
    table::Pager,
};

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

#[derive(Debug)]
pub struct LeafNode {
    pub bytes: Box<[u8; 4096]>,
}

impl LeafNode {
    const NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
    const NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
    const NEXT_LEAF_SIZE: usize = mem::size_of::<u32>();
    const NEXT_LEAF_OFFSET: usize = Self::NUM_CELLS_OFFSET + Self::NUM_CELLS_SIZE;
    const HEADER_SIZE: usize = Self::NEXT_LEAF_OFFSET + Self::NEXT_LEAF_SIZE;
    const KEY_SIZE: usize = mem::size_of::<u32>();
    const SPACE_FOR_CELLS: usize = 4096 - Self::HEADER_SIZE;

    pub fn new() -> Self {
        Self {
            bytes: vec![0u8; 4096].into_boxed_slice().try_into().unwrap(),
        }
    }

    pub fn new_with_bytes(bytes: Box<[u8; 4096]>) -> Self {
        Self { bytes }
    }

    pub fn cell_size(&self, value_size: usize) -> usize {
        Self::KEY_SIZE + value_size
    }

    pub fn max_cells(&self, value_size: usize) -> usize {
        (4096 - Self::HEADER_SIZE) / self.cell_size(value_size)
    }

    pub fn parent(&self) -> u32 {
        let bytes = self.bytes[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes)
    }

    pub fn set_parent(&mut self, val: u32) {
        self.bytes[PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
            .copy_from_slice(&val.to_ne_bytes())
    }

    pub fn next_leaf(&self) -> u32 {
        let bytes = self.bytes
            [Self::NEXT_LEAF_OFFSET..Self::NEXT_LEAF_OFFSET + Self::NEXT_LEAF_SIZE]
            .try_into()
            .unwrap();
        u32::from_le_bytes(bytes)
    }

    pub fn set_next_leaf(&mut self, val: u32) {
        self.bytes[Self::NEXT_LEAF_OFFSET..Self::NEXT_LEAF_OFFSET + Self::NEXT_LEAF_SIZE]
            .copy_from_slice(&val.to_ne_bytes())
    }

    pub fn num_cells(&self) -> u32 {
        u32::from_ne_bytes(
            self.bytes[Self::NUM_CELLS_OFFSET..Self::NUM_CELLS_OFFSET + Self::NUM_CELLS_SIZE]
                .try_into()
                .unwrap(),
        )
    }

    pub fn set_num_cells(&mut self, value: u32) {
        self.bytes[Self::NUM_CELLS_OFFSET..Self::NUM_CELLS_OFFSET + Self::NUM_CELLS_SIZE]
            .copy_from_slice(&value.to_ne_bytes())
    }

    pub fn key(&self, index: usize, value_size: usize) -> u32 {
        let offset: usize = Self::HEADER_SIZE + index * self.cell_size(value_size);
        let key = &self.bytes[offset..offset + Self::KEY_SIZE];
        u32::from_ne_bytes(key.try_into().unwrap())
    }

    pub fn read_row(&self, index: usize, schema: &Schema) -> (u32, Vec<ScalarValue>) {
        let value_size = schema.row_size();
        let mut offset = Self::HEADER_SIZE + index * self.cell_size(value_size);

        let key = &self.bytes[offset..offset + Self::KEY_SIZE];
        let key = u32::from_ne_bytes(key.try_into().unwrap());
        offset += Self::KEY_SIZE;

        let values_bytes = &self.bytes[offset..offset + value_size];
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
        (key, values)
    }

    pub fn cell_mut(&mut self, index: usize, value_size: usize) -> &mut [u8] {
        let cell_size = self.cell_size(value_size);
        let offset = Self::HEADER_SIZE + index * cell_size;
        &mut self.bytes[offset..offset + cell_size]
    }

    pub fn cell(&self, index: usize, value_size: usize) -> &[u8] {
        let cell_size = self.cell_size(value_size);
        let offset = Self::HEADER_SIZE + index * cell_size;
        &self.bytes[offset..offset + cell_size]
    }

    pub fn copy_within(&mut self, value_size: usize, src: usize, dst: usize) {
        let cell_size = self.cell_size(value_size);
        let offset_src = Self::HEADER_SIZE + src * cell_size;
        let offset_dst = Self::HEADER_SIZE + dst * cell_size;
        self.bytes
            .copy_within(offset_src..offset_src + cell_size, offset_dst)
    }

    pub fn serialize_row(
        &mut self,
        index: usize,
        schema: &Schema,
        key: u32,
        values: &[ScalarValue],
    ) {
        let value_size = schema.row_size();
        let cell = self.cell_mut(index, value_size);
        cell[..Self::KEY_SIZE].copy_from_slice(&key.to_ne_bytes());
        let mut cell_offset = Self::KEY_SIZE;

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

    fn leaf_node_split_and_insert<'a>(
        &mut self,
        key: u32,
        values: Vec<ScalarValue>,
        schema: &Schema,
    ) -> Option<LeafNode> {
        let value_size = schema.row_size();
        let max_cells = self.max_cells(value_size);
        let index = match self.binary_search(key, value_size) {
            Some(i) => i,
            None => 0,
        };

        let num_cells = self.num_cells();
        if num_cells < max_cells as u32 {
            for i in (index..self.num_cells() as usize).rev() {
                self.copy_within(value_size, i, i + 1);
            }
            self.serialize_row(index, schema, key, &values);
            self.set_num_cells(num_cells + 1);
            return None;
        }

        let mut new_node = LeafNode::new();
        new_node.set_parent(self.parent());
        new_node.set_next_leaf(self.next_leaf());
        // todo move this outside
        //self.set_next_leaf(page_index as u32);
        let leaf_node_right_split_count: usize = (max_cells + 1) / 2;
        let leaf_node_left_split_count = (max_cells + 1) - leaf_node_right_split_count;

        // Since there is one extra key in the keys that are to be places
        // We have max_cells + 1 keys .. but only max_cells - 1 real indexes in old page
        for i in 0..leaf_node_left_split_count {
            let index_within_node = i % leaf_node_left_split_count;
            if i == index {
                self.serialize_row(index_within_node, schema, key, &values);
            } else if i > index {
                // Copy cell at i - 1 to account for extra key
                self.copy_within(value_size, i - 1, index_within_node)
            } else {
                // Insert value has been serialized by now
                self.copy_within(value_size, i, index_within_node)
            }
        }

        for i in leaf_node_left_split_count..=max_cells {
            let index_within_node = i % leaf_node_left_split_count;
            if i == index {
                new_node.serialize_row(index_within_node, schema, key, &values);
            } else if i > index {
                // Copy cell at i - 1 to account for extra key
                new_node
                    .cell_mut(index_within_node, value_size)
                    .copy_from_slice(self.cell(i - 1, value_size));
            } else {
                // Insert value has been serialized by now
                new_node
                    .cell_mut(index_within_node, value_size)
                    .copy_from_slice(self.cell(i, value_size));
            }
        }

        self.set_num_cells(leaf_node_left_split_count as u32);
        new_node.set_num_cells(leaf_node_right_split_count as u32);

        Some(new_node)
    }

    pub fn binary_search(&self, key: u32, value_size: usize) -> Option<usize> {
        let mut left = 0;
        let mut right = self.num_cells() as usize;

        while left < right {
            let mid = left + (right - left) / 2;
            match self.key(mid, value_size).cmp(&key) {
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Equal => {
                    return Some(mid);
                }
                std::cmp::Ordering::Greater => {
                    right = mid;
                }
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct InternalNode {
    pub bytes: Box<[u8; 4096]>,
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
            self.bytes
                [Self::NODE_NUM_KEYS_OFFSET..Self::NODE_NUM_KEYS_OFFSET + Self::NODE_NUM_KEYS_SIZE]
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
        let bytes = self.bytes[Self::NODE_RIGHT_CHILD_OFFSET
            ..Self::NODE_RIGHT_CHILD_OFFSET + Self::NODE_RIGHT_CHILD_SIZE]
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
        let children_pointer_bytes = self.bytes[offset..offset + Self::NODE_CHILD_SIZE]
            .try_into()
            .unwrap();
        u32::from_ne_bytes(children_pointer_bytes)
    }

    pub(crate) fn new(bytes: Box<[u8; 4096]>) -> InternalNode {
        Self { bytes }
    }
}

#[cfg(test)]
mod test {
    use std::{env::temp_dir, fs::OpenOptions};

    use crate::{
        datatype::{DataType, ScalarValue, Schema},
        table::Pager,
    };

    use super::LeafNode;

    #[test]
    fn insert_one() {
        let schema = Schema {
            feilds: vec![("a".to_string(), DataType::Number)],
        };
        let mut page = LeafNode::new();
        assert_eq!(page.num_cells(), 0);
        page.leaf_node_split_and_insert(0, vec![ScalarValue::Number(1)], &schema);
        assert_eq!(page.num_cells(), 1);
        let (_, val) = page.read_row(0, &schema);
        assert_eq!(val, vec![ScalarValue::Number(1)])
    }

    #[test]
    fn insert_two() {
        let schema = Schema {
            feilds: vec![("a".to_string(), DataType::Number)],
        };
        let mut page = LeafNode::new();
        assert_eq!(page.num_cells(), 0);
        page.leaf_node_split_and_insert(1, vec![ScalarValue::Number(1)], &schema);
        page.leaf_node_split_and_insert(0, vec![ScalarValue::Number(2)], &schema);
        assert_eq!(page.num_cells(), 2);
        let (_, val) = page.read_row(0, &schema);
        assert_eq!(val, vec![ScalarValue::Number(2)]);
        let (_, val) = page.read_row(1, &schema);
        assert_eq!(val, vec![ScalarValue::Number(1)]);
    }

    #[test]
    fn fill_and_split() {
        let schema = Schema {
            feilds: vec![("a".to_string(), DataType::Number)],
        };
        let mut page = LeafNode::new();
        assert_eq!(page.num_cells(), 0);
        let value_size = schema.row_size();
        let max_cell = page.max_cells(value_size);

        for key in (0..max_cell).rev() {
            page.leaf_node_split_and_insert(
                key as u32,
                vec![ScalarValue::Number(key as i64)],
                &schema,
            );
            assert!(page.binary_search(key as u32, value_size).is_some());
            assert_eq!(page.num_cells(), (max_cell - key) as u32);
        }

        let new_node = page
            .leaf_node_split_and_insert(
                max_cell as u32,
                vec![ScalarValue::Number(max_cell as i64)],
                &schema,
            )
            .unwrap();

        assert_eq!(new_node.num_cells(), (max_cell as u32 + 1) / 2);
        assert_eq!(new_node.parent(), page.parent());
    }
}
