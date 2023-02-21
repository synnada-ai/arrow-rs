// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod async_writer;
#[cfg(test)]
mod test_utils;
mod writer;

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType};
pub use async_writer::AsyncWriter;
pub use async_writer::AsyncWriterBuilder;
pub use writer::Writer;
pub use writer::WriterBuilder;

pub const DEFAULT_DATE_FORMAT: &str = "%F";
pub const DEFAULT_TIME_FORMAT: &str = "%T";
pub const DEFAULT_TIMESTAMP_FORMAT: &str = "%FT%H:%M:%S.%9f";
pub const DEFAULT_TIMESTAMP_TZ_FORMAT: &str = "%FT%H:%M:%S.%9f%:z";
pub const DEFAULT_NULL_VALUE: &str = "";

pub fn get_header(batch: &RecordBatch) -> Vec<String> {
    let mut headers: Vec<String> = Vec::with_capacity(batch.num_columns());
    batch
        .schema()
        .fields()
        .iter()
        .for_each(|field| headers.push(field.name().to_string()));
    headers
}

pub fn get_converters<'a>(
    batch: &'a RecordBatch,
    options: &'a FormatOptions<'a>,
) -> Result<Vec<ArrayFormatter<'a>>, ArrowError> {
    batch
        .columns()
        .iter()
        .map(|a| match a.data_type() {
            d if d.is_nested() => Err(ArrowError::CsvError(format!(
                "Nested type {} is not supported in CSV",
                a.data_type()
            ))),
            DataType::Binary | DataType::LargeBinary => Err(ArrowError::CsvError(
                "Binary data cannot be written to CSV".to_string(),
            )),
            _ => ArrayFormatter::try_new(a.as_ref(), options),
        })
        .collect::<Result<Vec<_>, ArrowError>>()
}
