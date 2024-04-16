use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
    vec,
};

use crate::{
    datatype::{DataType, ScalarValue, Schema},
    errors::Error,
    statement::InsertStatement,
    TABLE_MAX_PAGE,
};

#[derive(Debug)]
pub struct Pager {
    file: File,
    pages: usize,
    cache: [Option<Box<Page>>; TABLE_MAX_PAGE],
}

const HEADER_SPACE: usize = 4096;

const NONE_VALUE: Option<Box<Page>> = None;
impl Pager {
    pub fn new(file: File, pages: u64) -> Result<Self, io::Error> {
        Ok(Self {
            file,
            pages: pages as usize,
            cache: [NONE_VALUE; TABLE_MAX_PAGE],
        })
    }

    pub fn page(&mut self, index: usize) -> Result<&mut Page, io::Error> {
        if index >= self.pages {
            self.file
                .set_len((self.pages + 1) as u64 * 4096 + HEADER_SPACE as u64)?;
            self.file.seek(std::io::SeekFrom::Start(
                index as u64 * 4096 + HEADER_SPACE as u64,
            ))?;
            self.pages += 1;
            self.cache[index] = Some(Box::new(Page::new()));
            return Ok(unsafe { (&mut self.cache[index]).as_deref_mut().unwrap_unchecked() });
        }

        match self.cache[index] {
            Some(ref mut page) => Ok(&mut *page),
            None => {
                self.file.seek(std::io::SeekFrom::Start(
                    index as u64 * 4096 + HEADER_SPACE as u64,
                ))?;
                let mut page = Page { bytes: [0u8; 4096] };
                self.file.read_exact(&mut page.bytes)?;
                self.cache[index] = Some(Box::new(page));
                Ok(unsafe { (&mut self.cache[index]).as_deref_mut().unwrap_unchecked() })
            }
        }
    }

    pub fn flush_page(&mut self, index: usize) -> Result<(), io::Error> {
        match self.cache[index] {
            Some(ref mut page) => {
                self.file.seek(io::SeekFrom::Start(
                    index as u64 * 4096 + HEADER_SPACE as u64,
                ))?;
                self.file.write_all(&page.bytes)?;
            }
            None => (),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Page {
    bytes: [u8; 4096],
}

impl Page {
    pub fn new() -> Page {
        Self { bytes: [0u8; 4096] }
    }

    pub fn read_row(&self, index: usize, schema: &Schema) -> Vec<ScalarValue> {
        let mut offset = index * schema.row_size();
        let mut values = Vec::new();
        for (_, ty) in &schema.feilds {
            let value = match ty {
                DataType::String(size) => {
                    let len = self.bytes[offset];
                    if len != 0 {
                        let bytes = &self.bytes[(offset + 1)..(offset + len as usize + 1)];
                        offset += size;
                        let string = String::from_utf8(bytes.to_owned()).unwrap();
                        ScalarValue::String(string)
                    } else {
                        ScalarValue::String("".to_string())
                    }
                }
                DataType::Number => {
                    let bytes = &self.bytes[offset..offset + 8];
                    offset += 8;
                    ScalarValue::Number(i64::from_ne_bytes(bytes.try_into().unwrap()))
                }
            };
            values.push(value);
        }
        values
    }

    pub fn write_row(&mut self, index: usize, schema: &Schema, values: Vec<ScalarValue>) {
        let mut page_offset = index * schema.row_size();
        let mut values = values.into_iter();

        for (_, ty) in &schema.feilds {
            match ty {
                DataType::String(size) => {
                    let ScalarValue::String(value) = values.next().unwrap() else {
                        panic!()
                    };
                    let bytes = &mut self.bytes[page_offset..page_offset + size];
                    bytes[0] = value.len() as u8;
                    (&mut bytes[1..]).write(value.as_bytes()).unwrap();
                    page_offset += size
                }
                DataType::Number => {
                    let ScalarValue::Number(value) = values.next().unwrap() else {
                        panic!()
                    };
                    (&mut self.bytes[page_offset..])
                        .write(&value.to_ne_bytes())
                        .unwrap();
                    page_offset += 8
                }
            };
        }
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

    pub fn insert(&mut self, values: InsertStatement) -> Result<(), Error> {
        let num_rows = self.header.num_rows;

        if num_rows >= self.max_rows() {
            return Err(Error::RowLimit);
        }
        let row_per_page = self.rows_per_page();
        let page_index = (num_rows + 1) / row_per_page;
        let page = self.pages.page(page_index)?;
        page.write_row(num_rows % row_per_page, &self.header.schema, values.values);
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
        let values = page.read_row(index, &self.header.schema);

        println!(
            "{}",
            values
                .iter()
                .map(|x| format!(" {} ", x))
                .collect::<String>()
        );

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

    use crate::{
        datatype::{DataType, ScalarValue, Schema},
        PAGE_SIZE,
    };

    use super::{Page, Pager, HEADER_SPACE};

    #[test]
    fn read_scalar_from_page() {
        let mut page = Page {
            bytes: [0u8; PAGE_SIZE],
        };

        let schema = Schema {
            feilds: vec![
                ("a".to_string(), DataType::Number),
                ("b".to_string(), DataType::String(10)),
            ],
        };

        let input = vec![
            ScalarValue::Number(1),
            ScalarValue::String("hello".to_string()),
        ];

        page.write_row(0, &schema, input.clone());

        let row = page.read_row(0, &schema);
        let mut values = row.into_iter();
        assert_eq!(values.next().unwrap(), input[0]);
        assert_eq!(values.next().unwrap(), input[1]);
    }

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
        let page = pager.page(0).unwrap();
        (&mut page.bytes).fill_with(|| 1u8);
        let page = pager.page(1).unwrap();
        (&mut page.bytes).fill_with(|| 2u8);
        pager.flush_page(0).unwrap();
        pager.flush_page(1).unwrap();
        pager.file.flush().unwrap();

        drop(pager);
        let mut pager = Pager::new(file, 2).unwrap();
        assert_eq!(&pager.page(0).unwrap().bytes, vec![1u8; 4096].as_slice());
        assert_eq!(&pager.page(1).unwrap().bytes, vec![2u8; 4096].as_slice());

        fs::remove_file(path).unwrap();
    }
}
