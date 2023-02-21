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

//! Async CSV Writer
//!
//! This CSV writer allows Arrow data (in record batches) to be written as CSV files leveraging
//! tokio::AsyncWrite trait.
//! The writer does not support writing `ListArray` and `StructArray`.
//!

use crate::map_async_csv_error;
use crate::writers::{
    get_converters, get_header, DEFAULT_DATE_FORMAT, DEFAULT_NULL_VALUE,
    DEFAULT_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_TZ_FORMAT, DEFAULT_TIME_FORMAT,
};
use arrow_array::RecordBatch;
use arrow_cast::display::FormatOptions;
use arrow_schema::ArrowError;
use csv_async::ByteRecord;
use tokio::io::AsyncWrite;

/// A Async CSV writer
#[derive(Debug)]
pub struct AsyncWriter<W: AsyncWrite + Unpin + Send> {
    /// The object to write to
    writer: csv_async::AsyncWriter<W>,
    /// Whether file should be written with headers. Defaults to `true`
    has_headers: bool,
    /// The date format for date arrays
    date_format: Option<String>,
    /// The datetime format for datetime arrays
    datetime_format: Option<String>,
    /// The timestamp format for timestamp arrays
    timestamp_format: Option<String>,
    /// The timestamp format for timestamp (with timezone) arrays
    timestamp_tz_format: Option<String>,
    /// The time format for time arrays
    time_format: Option<String>,
    /// Is the beginning-of-writer
    beginning: bool,
    /// The value to represent null entries
    null_value: String,
}

impl<W: AsyncWrite + Unpin + Send> AsyncWriter<W> {
    /// Create a new CsvWriter from a writable object, with default options
    pub fn new(writer: W) -> Self {
        let delimiter = b',';
        let writer = csv_async::AsyncWriterBuilder::new()
            .delimiter(delimiter)
            .create_writer(writer);
        AsyncWriter {
            writer,
            has_headers: true,
            date_format: Some(DEFAULT_DATE_FORMAT.to_string()),
            datetime_format: Some(DEFAULT_TIMESTAMP_FORMAT.to_string()),
            time_format: Some(DEFAULT_TIME_FORMAT.to_string()),
            timestamp_format: Some(DEFAULT_TIMESTAMP_FORMAT.to_string()),
            timestamp_tz_format: Some(DEFAULT_TIMESTAMP_TZ_FORMAT.to_string()),
            beginning: true,
            null_value: DEFAULT_NULL_VALUE.to_string(),
        }
    }

    /// Write a record batches to a writable object
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        if self.beginning {
            if self.has_headers {
                let headers = get_header(batch);
                self.writer
                    .write_record(&headers[..])
                    .await
                    .map_err(map_async_csv_error)?;
            }
            self.beginning = false;
        }

        let options = FormatOptions::default()
            .with_null(&self.null_value)
            .with_date_format(self.date_format.as_deref())
            .with_datetime_format(self.datetime_format.as_deref())
            .with_timestamp_format(self.timestamp_format.as_deref())
            .with_timestamp_tz_format(self.timestamp_tz_format.as_deref())
            .with_time_format(self.time_format.as_deref());

        let converters = get_converters(batch, &options)?;

        let mut buffer = String::with_capacity(1024);
        let mut byte_record = ByteRecord::with_capacity(1024, converters.len());

        for row_idx in 0..batch.num_rows() {
            byte_record.clear();
            for (col_idx, converter) in converters.iter().enumerate() {
                buffer.clear();
                converter.value(row_idx).write(&mut buffer).map_err(|e| {
                    ArrowError::CsvError(format!(
                        "Error formatting row {} and column {}: {e}",
                        row_idx + 1,
                        col_idx + 1
                    ))
                })?;
                byte_record.push_field(buffer.as_bytes());
            }
            self.writer
                .write_byte_record(&byte_record)
                .await
                .map_err(map_async_csv_error)?;
        }
        self.writer.flush().await?;

        Ok(())
    }
}

/// A CSV writer builder
#[derive(Debug)]
pub struct AsyncWriterBuilder {
    /// Optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// Whether to write column names as file headers. Defaults to `true`
    has_headers: bool,
    /// Whether to start from beginning.
    beginning: bool,
    /// Optional date format for date arrays
    date_format: Option<String>,
    /// Optional datetime format for datetime arrays
    datetime_format: Option<String>,
    /// Optional timestamp format for timestamp arrays
    timestamp_format: Option<String>,
    /// Optional timestamp format for timestamp with timezone arrays
    timestamp_tz_format: Option<String>,
    /// Optional time format for time arrays
    time_format: Option<String>,
    /// Optional value to represent null
    null_value: Option<String>,
}

impl Default for AsyncWriterBuilder {
    fn default() -> Self {
        Self {
            has_headers: true,
            beginning: true,
            delimiter: None,
            date_format: Some(DEFAULT_DATE_FORMAT.to_string()),
            datetime_format: Some(DEFAULT_TIMESTAMP_FORMAT.to_string()),
            time_format: Some(DEFAULT_TIME_FORMAT.to_string()),
            timestamp_format: Some(DEFAULT_TIMESTAMP_FORMAT.to_string()),
            timestamp_tz_format: Some(DEFAULT_TIMESTAMP_TZ_FORMAT.to_string()),
            null_value: Some(DEFAULT_NULL_VALUE.to_string()),
        }
    }
}

impl AsyncWriterBuilder {
    /// Create a new builder for configuring CSV writing options.
    ///
    /// To convert a builder into a writer, call `AsyncWriterBuilder::build`
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_csv::{AsyncWriter, AsyncWriterBuilder};
    /// # use tokio::fs::File;
    ///
    /// async fn example() -> AsyncWriter<File> {
    ///     let file = File::create("target/out.csv").await.unwrap();
    ///
    ///     // create a builder that doesn't write headers
    ///     let builder = AsyncWriterBuilder::new().has_headers(false).with_delimiter(b'|');
    ///     let writer = builder.build(file);
    ///
    ///     writer
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to write headers
    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    /// Set the CSV file's column delimiter as a byte character
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set the CSV file's date format
    pub fn with_date_format(mut self, format: String) -> Self {
        self.date_format = Some(format);
        self
    }

    /// Set the CSV file's datetime format
    pub fn with_datetime_format(mut self, format: String) -> Self {
        self.datetime_format = Some(format);
        self
    }

    /// Set the CSV file's time format
    pub fn with_time_format(mut self, format: String) -> Self {
        self.time_format = Some(format);
        self
    }

    /// Set the CSV file's timestamp format
    pub fn with_timestamp_format(mut self, format: String) -> Self {
        self.timestamp_format = Some(format);
        self
    }

    /// Set the value to represent null in output
    pub fn with_null(mut self, null_value: String) -> Self {
        self.null_value = Some(null_value);
        self
    }

    /// Use RFC3339 format for date/time/timestamps
    pub fn with_rfc3339(mut self) -> Self {
        self.date_format = None;
        self.datetime_format = None;
        self.time_format = None;
        self.timestamp_format = None;
        self.timestamp_tz_format = None;
        self
    }

    /// Create a new `Writer`
    pub fn build<W: AsyncWrite + Unpin + Send>(self, writer: W) -> AsyncWriter<W> {
        let writer = csv_async::AsyncWriterBuilder::new()
            .delimiter(self.delimiter.unwrap_or(b','))
            .create_writer(writer);
        AsyncWriter {
            writer,
            has_headers: self.has_headers,
            date_format: self.date_format,
            datetime_format: self.datetime_format,
            time_format: self.time_format,
            timestamp_format: self.timestamp_format,
            timestamp_tz_format: self.timestamp_tz_format,
            beginning: self.beginning,
            null_value: self
                .null_value
                .unwrap_or_else(|| DEFAULT_NULL_VALUE.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, SeekFrom};

    use crate::writers::test_utils::{
        test_conversion_consistency_case, test_write_csv_case,
        test_write_csv_custom_options_case, test_write_csv_decimal_case,
        test_write_csv_using_rfc3339_case,
    };
    use arrow_array::{Date32Array, Date64Array, TimestampNanosecondArray, UInt32Array};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    #[tokio::test]
    async fn test_writer() -> Result<(), ArrowError> {
        let (batches, expected) = test_write_csv_case()?;

        let mut file = async_tempfile::TempFile::new().await.unwrap();

        let mut writer = AsyncWriter::new(&mut file);
        for batch in batches {
            writer.write(&batch).await?;
        }
        drop(writer);

        // check that file was written successfully
        file.seek(SeekFrom::Start(0)).await?;
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(expected, String::from_utf8(buffer).unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_write_csv_decimal() -> Result<(), ArrowError> {
        let (batches, expected) = test_write_csv_decimal_case()?;

        let mut file = async_tempfile::TempFile::new().await.unwrap();

        let mut writer = AsyncWriter::new(&mut file);
        for batch in batches {
            writer.write(&batch).await?;
        }
        drop(writer);

        // check that file was written successfully
        file.seek(SeekFrom::Start(0)).await?;
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(expected, String::from_utf8(buffer).unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_write_csv_custom_options() -> Result<(), ArrowError> {
        let (batches, expected) = test_write_csv_custom_options_case()?;

        let mut file = async_tempfile::TempFile::new().await.unwrap();

        let builder = AsyncWriterBuilder::new()
            .has_headers(false)
            .with_delimiter(b'|')
            .with_null("NULL".to_string())
            .with_time_format("%r".to_string());
        let mut writer = builder.build(&mut file);
        for batch in batches {
            writer.write(&batch).await?;
        }
        drop(writer);

        // check that file was written successfully
        file.seek(SeekFrom::Start(0)).await?;
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(expected, String::from_utf8(buffer).unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_write_csv_invalid_cast() {
        let schema = Schema::new(vec![
            Field::new("c0", DataType::UInt32, false),
            Field::new("c1", DataType::Date64, false),
        ]);

        let c0 = UInt32Array::from(vec![Some(123), Some(234)]);
        let c1 = Date64Array::from(vec![Some(1926632005177), Some(1926632005177685347)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c0), Arc::new(c1)])
                .unwrap();

        let mut file = async_tempfile::TempFile::new().await.unwrap();
        let mut writer = AsyncWriter::new(&mut file);
        let batches = vec![&batch, &batch];
        for batch in batches {
            let err = writer.write(batch).await.unwrap_err().to_string();
            assert_eq!(err, "Csv error: Error formatting row 2 and column 2: Cast error: Failed to convert 1926632005177685347 to temporal for Date64")
        }
        drop(writer);
    }

    #[tokio::test]
    async fn test_conversion_consistency() -> Result<(), ArrowError> {
        // test if we can serialize and deserialize whilst retaining the same type information/ precision
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Date32, false),
            Field::new("c2", DataType::Date64, false),
            Field::new("c3", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ]);

        let nanoseconds = vec![
            1599566300000000000,
            1599566200000000000,
            1599566100000000000,
        ];
        let c1 = Date32Array::from(vec![3, 2, 1]);
        let c2 = Date64Array::from(vec![3, 2, 1]);
        let c3 = TimestampNanosecondArray::from(nanoseconds.clone());

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3)],
        )?;

        let builder = AsyncWriterBuilder::new().has_headers(false);

        let mut buf: Cursor<Vec<u8>> = Default::default();
        // drop the writer early to release the borrow.
        {
            let mut writer = builder.build(&mut buf);
            writer.write(&batch).await?;
        }
        buf.set_position(0);
        test_conversion_consistency_case(&mut buf, Arc::new(schema), nanoseconds)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_write_csv_using_rfc3339() -> Result<(), ArrowError> {
        let (batches, expected) = test_write_csv_using_rfc3339_case()?;

        let mut file = async_tempfile::TempFile::new().await.unwrap();

        let builder = AsyncWriterBuilder::new().with_rfc3339();
        let mut writer = builder.build(&mut file);
        for batch in batches {
            writer.write(&batch).await?;
        }
        drop(writer);

        file.seek(SeekFrom::Start(0)).await?;
        let mut buffer: Vec<u8> = vec![];
        file.read_to_end(&mut buffer).await?;

        assert_eq!(expected, String::from_utf8(buffer).unwrap());
        Ok(())
    }
}
