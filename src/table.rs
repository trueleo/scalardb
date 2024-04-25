use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
    vec,
};

use crate::{
    datatype::Schema,
    errors::Error,
    statement::InsertStatement,
    tree::{InternalNode, LeafNode},
    TABLE_MAX_PAGE,
};

#[derive(Debug)]
pub enum Page {
    Leaf(LeafNode),
    Intermediate(InternalNode),
}

impl Page {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Page::Leaf(x) => &*x.bytes,
            Page::Intermediate(x) => &*x.bytes,
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        match self {
            Page::Leaf(x) => &mut *x.bytes,
            Page::Intermediate(x) => &mut *x.bytes,
        }
    }
}

#[derive(Debug)]
pub struct Pager {
    file: File,
    pages: usize,
    cache: [Option<Page>; TABLE_MAX_PAGE],
}

const HEADER_SPACE: usize = 4096;

const NONE_VALUE: Option<Page> = None;
impl Pager {
    pub fn new(file: File, pages: u64) -> Result<Self, io::Error> {
        Ok(Self {
            file,
            pages: pages as usize,
            cache: [NONE_VALUE; TABLE_MAX_PAGE],
        })
    }

    pub fn new_leaf_page(&mut self) -> Result<(u32, &mut LeafNode), io::Error> {
        let index = self.pages;
        self.file
            .set_len((self.pages + 1) as u64 * 4096 + HEADER_SPACE as u64)?;
        self.file.seek(std::io::SeekFrom::Start(
            index as u64 * 4096 + HEADER_SPACE as u64,
        ))?;
        self.pages += 1;
        let page = vec![0u8; 4096].into_boxed_slice().try_into().unwrap();
        self.cache[index] = Some(Page::Leaf(LeafNode::new(page)));
        let Page::Leaf(page) = self.cache[index].as_mut().unwrap() else {
            unreachable!()
        };
        return Ok((index as u32, page));
    }

    pub fn page(&mut self, index: usize) -> Result<&mut Page, io::Error> {
        match self.cache[index] {
            Some(ref mut page) => Ok(&mut *page),
            None => {
                self.file.seek(std::io::SeekFrom::Start(
                    index as u64 * 4096 + HEADER_SPACE as u64,
                ))?;
                let mut page: Box<[u8; 4096]> =
                    vec![0u8; 4096].into_boxed_slice().try_into().unwrap();
                self.file.read_exact(&mut *page)?;
                let page = match page[0] {
                    0 => Page::Leaf(LeafNode::new(page)),
                    1 => Page::Intermediate(InternalNode::new(page)),
                    _ => unreachable!(),
                };
                self.cache[index] = Some(page);
                Ok(unsafe { (&mut self.cache[index]).as_mut().unwrap_unchecked() })
            }
        }
    }

    pub fn flush_page(&mut self, index: usize) -> Result<(), io::Error> {
        match self.cache[index] {
            Some(ref mut page) => {
                self.file.seek(io::SeekFrom::Start(
                    index as u64 * 4096 + HEADER_SPACE as u64,
                ))?;
                self.file.write_all(page.bytes())?;
            }
            None => (),
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TableHeader {
    pub name: String,
    pub schema: Schema,
    pub num_rows: usize,
}

#[derive(Debug)]
pub struct Table {
    pub header: TableHeader,
    pub pages: Pager,
}

impl Table {
    pub fn new(name: String, schema: Schema, path: &Path) -> Result<Self, Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if file.metadata()?.len() == 0 {
            let header = TableHeader {
                name,
                schema: schema.clone(),
                num_rows: 0,
            };
            let mut buffer = vec![0u8; HEADER_SPACE];
            bincode::serialize_into(&mut buffer[..], &header).unwrap();
            dbg!(bincode::serialized_size(&header).unwrap());
            dbg!(bincode::deserialize::<TableHeader>(&buffer).unwrap());

            file.seek(io::SeekFrom::Start(0))?;
            file.write_all(&buffer)?;
        }

        dbg!(schema.row_size());

        file.seek(io::SeekFrom::Start(0))?;
        let mut header = vec![0u8; HEADER_SPACE];
        file.read_exact(&mut header[..])?;
        let header: TableHeader = bincode::deserialize(&header).unwrap();
        dbg!(&header.schema);
        let pages = header
            .num_rows
            .div_ceil(crate::PAGE_SIZE / header.schema.row_size());
        Ok(Self {
            header,
            pages: Pager::new(file, pages as u64)?,
        })
    }

    pub fn insert(&mut self, _values: InsertStatement) -> Result<(), Error> {
        let num_rows = self.header.num_rows;

        if num_rows >= self.max_rows() {
            return Err(Error::RowLimit);
        }

        let row_per_page = self.rows_per_page();
        let page_index = (num_rows + 1) / row_per_page;
        let page = self.pages.page(page_index)?;
        todo!("insert value");
        self.pages.flush_page(page_index)?;
        self.header.num_rows += 1;
        self.flush_table_header()?;
        self.pages.file.flush()?;
        Ok(())
    }

    pub fn read(&mut self, index: usize) -> Result<(), Error> {
        let page_index = (self.header.num_rows + 1) / self.rows_per_page();
        let index = index % self.rows_per_page();
        let page = self.pages.page(page_index)?;
        todo!("read row");
        // println!(
        //     "{}",
        //     values
        //         .iter()
        //         .map(|x| format!(" {} ", x))
        //         .collect::<String>()
        // );

        Ok(())
    }

    pub fn flush_table_header(&mut self) -> Result<(), Error> {
        let mut buf = vec![0u8; HEADER_SPACE];
        bincode::serialize_into(&mut buf[..], &self.header)?;
        self.pages.file.seek(io::SeekFrom::Start(0))?;
        self.pages.file.write_all(&buf[..])?;
        Ok(())
    }

    pub fn rows_per_page(&self) -> usize {
        let row_size = self.header.schema.row_size();
        crate::PAGE_SIZE / row_size
    }

    pub fn max_rows(&self) -> usize {
        self.rows_per_page() * crate::TABLE_MAX_PAGE
    }

    pub fn schema(&self) -> &Schema {
        &self.header.schema
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
    };

    use super::{Pager, HEADER_SPACE};

    #[test]
    fn pager_test() {
        let path = std::env::temp_dir().join("glob.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();

        file.set_len(HEADER_SPACE as u64).unwrap();
        let mut pager = Pager::new(file.try_clone().unwrap(), 0).unwrap();
        let (_, page) = pager.new_leaf_page().unwrap();
        (&mut *page.bytes).fill_with(|| 1u8);
        let (_, page) = pager.new_leaf_page().unwrap();
        (&mut *page.bytes).fill_with(|| 2u8);
        pager.flush_page(0).unwrap();
        pager.flush_page(1).unwrap();
        pager.file.flush().unwrap();

        drop(pager);
        let mut pager = Pager::new(file, 2).unwrap();
        assert_eq!(pager.page(0).unwrap().bytes(), vec![1u8; 4096].as_slice());
        assert_eq!(pager.page(1).unwrap().bytes(), vec![2u8; 4096].as_slice());

        fs::remove_file(path).unwrap();
    }
}
