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

//! Configuration via [`WriterProperties`] and [`ReaderProperties`]
use crate::basic::{Compression, Encoding};
use crate::compression::{CodecOptions, CodecOptionsBuilder};
#[cfg(feature = "encryption")]
use crate::encryption::encrypt::FileEncryptionProperties;
use crate::file::metadata::KeyValue;
use crate::format::SortingColumn;
use crate::schema::types::ColumnPath;
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};

/// Default value for [`WriterProperties::data_page_size_limit`]
pub const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;
/// Default value for [`WriterProperties::write_batch_size`]
pub const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;
/// Default value for [`WriterProperties::writer_version`]
pub const DEFAULT_WRITER_VERSION: WriterVersion = WriterVersion::PARQUET_1_0;
/// Default value for [`WriterProperties::compression`]
pub const DEFAULT_COMPRESSION: Compression = Compression::UNCOMPRESSED;
/// Default value for [`WriterProperties::dictionary_enabled`]
pub const DEFAULT_DICTIONARY_ENABLED: bool = true;
/// Default value for [`WriterProperties::dictionary_page_size_limit`]
pub const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT: usize = DEFAULT_PAGE_SIZE;
/// Default value for [`WriterProperties::data_page_row_count_limit`]
pub const DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT: usize = 20_000;
/// Default value for [`WriterProperties::statistics_enabled`]
pub const DEFAULT_STATISTICS_ENABLED: EnabledStatistics = EnabledStatistics::Page;
/// Default value for [`WriterProperties::max_statistics_size`]
#[deprecated(since = "54.0.0", note = "Unused; will be removed in 56.0.0")]
pub const DEFAULT_MAX_STATISTICS_SIZE: usize = 4096;
/// Default value for [`WriterProperties::max_row_group_size`]
pub const DEFAULT_MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;
/// Default value for [`WriterProperties::bloom_filter_position`]
pub const DEFAULT_BLOOM_FILTER_POSITION: BloomFilterPosition = BloomFilterPosition::AfterRowGroup;
/// Default value for [`WriterProperties::created_by`]
pub const DEFAULT_CREATED_BY: &str = concat!("parquet-rs version ", env!("CARGO_PKG_VERSION"));
/// Default value for [`WriterProperties::column_index_truncate_length`]
pub const DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH: Option<usize> = Some(64);
/// Default value for [`BloomFilterProperties::fpp`]
pub const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.05;
/// Default value for [`BloomFilterProperties::ndv`]
pub const DEFAULT_BLOOM_FILTER_NDV: u64 = 1_000_000_u64;
/// Default values for [`WriterProperties::statistics_truncate_length`]
pub const DEFAULT_STATISTICS_TRUNCATE_LENGTH: Option<usize> = None;
/// Default value for [`WriterProperties::offset_index_disabled`]
pub const DEFAULT_OFFSET_INDEX_DISABLED: bool = false;
/// Default values for [`WriterProperties::coerce_types`]
pub const DEFAULT_COERCE_TYPES: bool = false;

/// Parquet writer version.
///
/// Basic constant, which is not part of the Thrift definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum WriterVersion {
    /// Parquet format version 1.0
    PARQUET_1_0,
    /// Parquet format version 2.0
    PARQUET_2_0,
}

impl WriterVersion {
    /// Returns writer version as `i32`.
    pub fn as_num(&self) -> i32 {
        match self {
            WriterVersion::PARQUET_1_0 => 1,
            WriterVersion::PARQUET_2_0 => 2,
        }
    }
}

impl FromStr for WriterVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PARQUET_1_0" | "parquet_1_0" => Ok(WriterVersion::PARQUET_1_0),
            "PARQUET_2_0" | "parquet_2_0" => Ok(WriterVersion::PARQUET_2_0),
            _ => Err(format!("Invalid writer version: {s}")),
        }
    }
}

/// Where in the file [`ArrowWriter`](crate::arrow::arrow_writer::ArrowWriter) should
/// write Bloom filters
///
/// Basic constant, which is not part of the Thrift definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomFilterPosition {
    /// Write Bloom Filters of each row group right after the row group
    ///
    /// This saves memory by writing it as soon as it is computed, at the cost
    /// of data locality for readers
    AfterRowGroup,
    /// Write Bloom Filters at the end of the file
    ///
    /// This allows better data locality for readers, at the cost of memory usage
    /// for writers.
    End,
}

/// Reference counted writer properties.
pub type WriterPropertiesPtr = Arc<WriterProperties>;

/// Configuration settings for writing parquet files.
///
/// Use [`Self::builder`] to create a [`WriterPropertiesBuilder`] to change settings.
///
/// # Example
///
/// ```rust
/// # use parquet::{
/// #    basic::{Compression, Encoding},
/// #    file::properties::*,
/// #    schema::types::ColumnPath,
/// # };
/// #
/// // Create properties with default configuration.
/// let props = WriterProperties::default();
///
/// // Use properties builder to set certain options and assemble the configuration.
/// let props = WriterProperties::builder()
///     .set_writer_version(WriterVersion::PARQUET_1_0)
///     .set_encoding(Encoding::PLAIN)
///     .set_column_encoding(ColumnPath::from("col1"), Encoding::DELTA_BINARY_PACKED)
///     .set_compression(Compression::SNAPPY)
///     .build();
///
/// assert_eq!(props.writer_version(), WriterVersion::PARQUET_1_0);
/// assert_eq!(
///     props.encoding(&ColumnPath::from("col1")),
///     Some(Encoding::DELTA_BINARY_PACKED)
/// );
/// assert_eq!(
///     props.encoding(&ColumnPath::from("col2")),
///     Some(Encoding::PLAIN)
/// );
/// ```
#[derive(Debug, Clone)]
pub struct WriterProperties {
    data_page_size_limit: usize,
    data_page_row_count_limit: usize,
    write_batch_size: usize,
    max_row_group_size: usize,
    bloom_filter_position: BloomFilterPosition,
    writer_version: WriterVersion,
    created_by: String,
    offset_index_disabled: bool,
    pub(crate) key_value_metadata: Option<Vec<KeyValue>>,
    default_column_properties: ColumnProperties,
    column_properties: HashMap<ColumnPath, ColumnProperties>,
    sorting_columns: Option<Vec<SortingColumn>>,
    column_index_truncate_length: Option<usize>,
    statistics_truncate_length: Option<usize>,
    coerce_types: bool,
    #[cfg(feature = "encryption")]
    pub(crate) file_encryption_properties: Option<FileEncryptionProperties>,
}

impl Default for WriterProperties {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl WriterProperties {
    /// Create a new [`WriterProperties`] with the default settings
    ///
    /// See [`WriterProperties::builder`] for customising settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a new default [`WriterPropertiesBuilder`] for creating writer
    /// properties.
    pub fn builder() -> WriterPropertiesBuilder {
        WriterPropertiesBuilder::with_defaults()
    }

    /// Returns data page size limit.
    ///
    /// Note: this is a best effort limit based on the write batch size
    ///
    /// For more details see [`WriterPropertiesBuilder::set_data_page_size_limit`]
    pub fn data_page_size_limit(&self) -> usize {
        self.data_page_size_limit
    }

    /// Returns dictionary page size limit.
    ///
    /// Note: this is a best effort limit based on the write batch size
    ///
    /// For more details see [`WriterPropertiesBuilder::set_dictionary_page_size_limit`]
    pub fn dictionary_page_size_limit(&self) -> usize {
        self.default_column_properties
            .dictionary_page_size_limit()
            .unwrap_or(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT)
    }

    /// Returns dictionary page size limit for a specific column.
    pub fn column_dictionary_page_size_limit(&self, col: &ColumnPath) -> usize {
        self.column_properties
            .get(col)
            .and_then(|c| c.dictionary_page_size_limit())
            .or_else(|| self.default_column_properties.dictionary_page_size_limit())
            .unwrap_or(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT)
    }

    /// Returns the maximum page row count
    ///
    /// Note: this is a best effort limit based on the write batch size
    ///
    /// For more details see [`WriterPropertiesBuilder::set_data_page_row_count_limit`]
    pub fn data_page_row_count_limit(&self) -> usize {
        self.data_page_row_count_limit
    }

    /// Returns configured batch size for writes.
    ///
    /// When writing a batch of data, this setting allows to split it internally into
    /// smaller batches so we can better estimate the size of a page currently being
    /// written.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_write_batch_size`]
    pub fn write_batch_size(&self) -> usize {
        self.write_batch_size
    }

    /// Returns maximum number of rows in a row group.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_max_row_group_size`]
    pub fn max_row_group_size(&self) -> usize {
        self.max_row_group_size
    }

    /// Returns bloom filter position.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_bloom_filter_position`]
    pub fn bloom_filter_position(&self) -> BloomFilterPosition {
        self.bloom_filter_position
    }

    /// Returns configured writer version.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_writer_version`]
    pub fn writer_version(&self) -> WriterVersion {
        self.writer_version
    }

    /// Returns `created_by` string.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_created_by`]
    pub fn created_by(&self) -> &str {
        &self.created_by
    }

    /// Returns `true` if offset index writing is disabled.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_offset_index_disabled`]
    pub fn offset_index_disabled(&self) -> bool {
        // If page statistics are to be collected, then do not disable the offset indexes.
        let default_page_stats_enabled =
            self.default_column_properties.statistics_enabled() == Some(EnabledStatistics::Page);
        let column_page_stats_enabled = self
            .column_properties
            .iter()
            .any(|path_props| path_props.1.statistics_enabled() == Some(EnabledStatistics::Page));
        if default_page_stats_enabled || column_page_stats_enabled {
            return false;
        }

        self.offset_index_disabled
    }

    /// Returns `key_value_metadata` KeyValue pairs.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_key_value_metadata`]
    pub fn key_value_metadata(&self) -> Option<&Vec<KeyValue>> {
        self.key_value_metadata.as_ref()
    }

    /// Returns sorting columns.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_sorting_columns`]
    pub fn sorting_columns(&self) -> Option<&Vec<SortingColumn>> {
        self.sorting_columns.as_ref()
    }

    /// Returns the maximum length of truncated min/max values in the column index.
    ///
    /// `None` if truncation is disabled, must be greater than 0 otherwise.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_column_index_truncate_length`]
    pub fn column_index_truncate_length(&self) -> Option<usize> {
        self.column_index_truncate_length
    }

    /// Returns the maximum length of truncated min/max values in [`Statistics`].
    ///
    /// `None` if truncation is disabled, must be greater than 0 otherwise.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_statistics_truncate_length`]
    ///
    /// [`Statistics`]: crate::file::statistics::Statistics
    pub fn statistics_truncate_length(&self) -> Option<usize> {
        self.statistics_truncate_length
    }

    /// Returns `true` if type coercion is enabled.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_coerce_types`]
    pub fn coerce_types(&self) -> bool {
        self.coerce_types
    }

    /// Returns encoding for a data page, when dictionary encoding is enabled.
    ///
    /// This is not configurable.
    #[inline]
    pub fn dictionary_data_page_encoding(&self) -> Encoding {
        // PLAIN_DICTIONARY encoding is deprecated in writer version 1.
        // Dictionary values are encoded using RLE_DICTIONARY encoding.
        Encoding::RLE_DICTIONARY
    }

    /// Returns encoding for dictionary page, when dictionary encoding is enabled.
    ///
    /// This is not configurable.
    #[inline]
    pub fn dictionary_page_encoding(&self) -> Encoding {
        // PLAIN_DICTIONARY is deprecated in writer version 1.
        // Dictionary is encoded using plain encoding.
        Encoding::PLAIN
    }

    /// Returns encoding for a column, if set.
    ///
    /// In case when dictionary is enabled, returns fallback encoding.
    ///
    /// If encoding is not set, then column writer will choose the best encoding
    /// based on the column type.
    pub fn encoding(&self, col: &ColumnPath) -> Option<Encoding> {
        self.column_properties
            .get(col)
            .and_then(|c| c.encoding())
            .or_else(|| self.default_column_properties.encoding())
    }

    /// Returns compression codec for a column.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_column_compression`]
    pub fn compression(&self, col: &ColumnPath) -> Compression {
        self.column_properties
            .get(col)
            .and_then(|c| c.compression())
            .or_else(|| self.default_column_properties.compression())
            .unwrap_or(DEFAULT_COMPRESSION)
    }

    /// Returns `true` if dictionary encoding is enabled for a column.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_dictionary_enabled`]
    pub fn dictionary_enabled(&self, col: &ColumnPath) -> bool {
        self.column_properties
            .get(col)
            .and_then(|c| c.dictionary_enabled())
            .or_else(|| self.default_column_properties.dictionary_enabled())
            .unwrap_or(DEFAULT_DICTIONARY_ENABLED)
    }

    /// Returns which statistics are written for a column.
    ///
    /// For more details see [`WriterPropertiesBuilder::set_statistics_enabled`]
    pub fn statistics_enabled(&self, col: &ColumnPath) -> EnabledStatistics {
        self.column_properties
            .get(col)
            .and_then(|c| c.statistics_enabled())
            .or_else(|| self.default_column_properties.statistics_enabled())
            .unwrap_or(DEFAULT_STATISTICS_ENABLED)
    }

    /// Returns max size for statistics.
    ///
    /// UNUSED
    #[deprecated(since = "54.0.0", note = "Unused; will be removed in 56.0.0")]
    pub fn max_statistics_size(&self, col: &ColumnPath) -> usize {
        #[allow(deprecated)]
        self.column_properties
            .get(col)
            .and_then(|c| c.max_statistics_size())
            .or_else(|| self.default_column_properties.max_statistics_size())
            .unwrap_or(DEFAULT_MAX_STATISTICS_SIZE)
    }

    /// Returns the [`BloomFilterProperties`] for the given column
    ///
    /// Returns `None` if bloom filter is disabled
    ///
    /// For more details see [`WriterPropertiesBuilder::set_column_bloom_filter_enabled`]
    pub fn bloom_filter_properties(&self, col: &ColumnPath) -> Option<&BloomFilterProperties> {
        self.column_properties
            .get(col)
            .and_then(|c| c.bloom_filter_properties())
            .or_else(|| self.default_column_properties.bloom_filter_properties())
    }

    /// Return file encryption properties
    ///
    /// For more details see [`WriterPropertiesBuilder::with_file_encryption_properties`]
    #[cfg(feature = "encryption")]
    pub fn file_encryption_properties(&self) -> Option<&FileEncryptionProperties> {
        self.file_encryption_properties.as_ref()
    }
}

/// Builder for  [`WriterProperties`] Parquet writer configuration.
///
/// See example on [`WriterProperties`]
pub struct WriterPropertiesBuilder {
    data_page_size_limit: usize,
    data_page_row_count_limit: usize,
    write_batch_size: usize,
    max_row_group_size: usize,
    bloom_filter_position: BloomFilterPosition,
    writer_version: WriterVersion,
    created_by: String,
    offset_index_disabled: bool,
    key_value_metadata: Option<Vec<KeyValue>>,
    default_column_properties: ColumnProperties,
    column_properties: HashMap<ColumnPath, ColumnProperties>,
    sorting_columns: Option<Vec<SortingColumn>>,
    column_index_truncate_length: Option<usize>,
    statistics_truncate_length: Option<usize>,
    coerce_types: bool,
    #[cfg(feature = "encryption")]
    file_encryption_properties: Option<FileEncryptionProperties>,
}

impl WriterPropertiesBuilder {
    /// Returns default state of the builder.
    fn with_defaults() -> Self {
        Self {
            data_page_size_limit: DEFAULT_PAGE_SIZE,
            data_page_row_count_limit: DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT,
            write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            bloom_filter_position: DEFAULT_BLOOM_FILTER_POSITION,
            writer_version: DEFAULT_WRITER_VERSION,
            created_by: DEFAULT_CREATED_BY.to_string(),
            offset_index_disabled: DEFAULT_OFFSET_INDEX_DISABLED,
            key_value_metadata: None,
            default_column_properties: Default::default(),
            column_properties: HashMap::new(),
            sorting_columns: None,
            column_index_truncate_length: DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
            statistics_truncate_length: DEFAULT_STATISTICS_TRUNCATE_LENGTH,
            coerce_types: DEFAULT_COERCE_TYPES,
            #[cfg(feature = "encryption")]
            file_encryption_properties: None,
        }
    }

    /// Finalizes the configuration and returns immutable writer properties struct.
    pub fn build(self) -> WriterProperties {
        WriterProperties {
            data_page_size_limit: self.data_page_size_limit,
            data_page_row_count_limit: self.data_page_row_count_limit,
            write_batch_size: self.write_batch_size,
            max_row_group_size: self.max_row_group_size,
            bloom_filter_position: self.bloom_filter_position,
            writer_version: self.writer_version,
            created_by: self.created_by,
            offset_index_disabled: self.offset_index_disabled,
            key_value_metadata: self.key_value_metadata,
            default_column_properties: self.default_column_properties,
            column_properties: self.column_properties,
            sorting_columns: self.sorting_columns,
            column_index_truncate_length: self.column_index_truncate_length,
            statistics_truncate_length: self.statistics_truncate_length,
            coerce_types: self.coerce_types,
            #[cfg(feature = "encryption")]
            file_encryption_properties: self.file_encryption_properties,
        }
    }

    // ----------------------------------------------------------------------
    // Writer properties related to a file

    /// Sets the `WriterVersion` written into the parquet metadata (defaults to [`PARQUET_1_0`]
    /// via [`DEFAULT_WRITER_VERSION`])
    ///
    /// This value can determine what features some readers will support.
    ///
    /// [`PARQUET_1_0`]: [WriterVersion::PARQUET_1_0]
    pub fn set_writer_version(mut self, value: WriterVersion) -> Self {
        self.writer_version = value;
        self
    }

    /// Sets best effort maximum size of a data page in bytes (defaults to `1024 * 1024`
    /// via [`DEFAULT_PAGE_SIZE`]).
    ///
    /// The parquet writer will attempt to limit the sizes of each
    /// `DataPage` to this many bytes. Reducing this value will result
    /// in larger parquet files, but may improve the effectiveness of
    /// page index based predicate pushdown during reading.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    pub fn set_data_page_size_limit(mut self, value: usize) -> Self {
        self.data_page_size_limit = value;
        self
    }

    /// Sets best effort maximum number of rows in a data page (defaults to `20_000`
    /// via [`DEFAULT_DATA_PAGE_ROW_COUNT_LIMIT`]).
    ///
    /// The parquet writer will attempt to limit the number of rows in
    /// each `DataPage` to this value. Reducing this value will result
    /// in larger parquet files, but may improve the effectiveness of
    /// page index based predicate pushdown during reading.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    pub fn set_data_page_row_count_limit(mut self, value: usize) -> Self {
        self.data_page_row_count_limit = value;
        self
    }

    /// Sets best effort maximum dictionary page size, in bytes (defaults to `1024 * 1024`
    /// via [`DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT`]).
    ///
    /// The parquet writer will attempt to limit the size of each
    /// `DataPage` used to store dictionaries to this many
    /// bytes. Reducing this value will result in larger parquet
    /// files, but may improve the effectiveness of page index based
    /// predicate pushdown during reading.
    ///
    /// Note: this is a best effort limit based on value of
    /// [`set_write_batch_size`](Self::set_write_batch_size).
    pub fn set_dictionary_page_size_limit(mut self, value: usize) -> Self {
        self.default_column_properties
            .set_dictionary_page_size_limit(value);
        self
    }

    /// Sets write batch size (defaults to 1024 via [`DEFAULT_WRITE_BATCH_SIZE`]).
    ///
    /// For performance reasons, data for each column is written in
    /// batches of this size.
    ///
    /// Additional limits such as such as
    /// [`set_data_page_row_count_limit`](Self::set_data_page_row_count_limit)
    /// are checked between batches, and thus the write batch size value acts as an
    /// upper-bound on the enforcement granularity of other limits.
    pub fn set_write_batch_size(mut self, value: usize) -> Self {
        self.write_batch_size = value;
        self
    }

    /// Sets maximum number of rows in a row group (defaults to `1024 * 1024`
    /// via [`DEFAULT_MAX_ROW_GROUP_SIZE`]).
    ///
    /// # Panics
    /// If the value is set to 0.
    pub fn set_max_row_group_size(mut self, value: usize) -> Self {
        assert!(value > 0, "Cannot have a 0 max row group size");
        self.max_row_group_size = value;
        self
    }

    /// Sets where in the final file Bloom Filters are written (defaults to  [`AfterRowGroup`]
    /// via [`DEFAULT_BLOOM_FILTER_POSITION`])
    ///
    /// [`AfterRowGroup`]: BloomFilterPosition::AfterRowGroup
    pub fn set_bloom_filter_position(mut self, value: BloomFilterPosition) -> Self {
        self.bloom_filter_position = value;
        self
    }

    /// Sets "created by" property (defaults to `parquet-rs version <VERSION>` via
    /// [`DEFAULT_CREATED_BY`]).
    ///
    /// This is a string that will be written into the file metadata
    pub fn set_created_by(mut self, value: String) -> Self {
        self.created_by = value;
        self
    }

    /// Sets whether the writing of offset indexes is disabled (defaults to `false` via
    /// [`DEFAULT_OFFSET_INDEX_DISABLED`]).
    ///
    /// If statistics level is set to [`Page`] this setting will be overridden with `false`.
    ///
    /// Note: As the offset indexes are useful for accessing data by row number,
    /// they are always written by default, regardless of whether other statistics
    /// are enabled. Disabling this metadata may result in a degradation in read
    /// performance, so use this option with care.
    ///
    /// [`Page`]: EnabledStatistics::Page
    pub fn set_offset_index_disabled(mut self, value: bool) -> Self {
        self.offset_index_disabled = value;
        self
    }

    /// Sets "key_value_metadata" property (defaults to `None`).
    pub fn set_key_value_metadata(mut self, value: Option<Vec<KeyValue>>) -> Self {
        self.key_value_metadata = value;
        self
    }

    /// Sets sorting order of rows in the row group if any (defaults to `None`).
    pub fn set_sorting_columns(mut self, value: Option<Vec<SortingColumn>>) -> Self {
        self.sorting_columns = value;
        self
    }

    /// Sets the max length of min/max value fields when writing the column
    /// [`Index`] (defaults to `Some(64)` via [`DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH`]).
    ///
    /// This can be used to prevent columns with very long values (hundreds of
    /// bytes long) from causing the parquet metadata to become huge.
    ///
    /// # Notes
    ///
    /// The column [`Index`] is written when [`Self::set_statistics_enabled`] is
    /// set to [`EnabledStatistics::Page`].
    ///
    /// * If `Some`, must be greater than 0, otherwise will panic
    /// * If `None`, there's no effective limit.
    ///
    /// [`Index`]: crate::file::page_index::index::Index
    pub fn set_column_index_truncate_length(mut self, max_length: Option<usize>) -> Self {
        if let Some(value) = max_length {
            assert!(value > 0, "Cannot have a 0 column index truncate length. If you wish to disable min/max value truncation, set it to `None`.");
        }

        self.column_index_truncate_length = max_length;
        self
    }

    /// Sets the max length of min/max value fields in row group and data page header
    /// [`Statistics`] (defaults to `None` (no limit) via [`DEFAULT_STATISTICS_TRUNCATE_LENGTH`]).
    ///
    /// # Notes
    /// Row group [`Statistics`] are written when [`Self::set_statistics_enabled`] is
    /// set to [`EnabledStatistics::Chunk`] or [`EnabledStatistics::Page`]. Data page header
    /// [`Statistics`] are written when [`Self::set_statistics_enabled`] is set to
    /// [`EnabledStatistics::Page`].
    ///
    /// * If `Some`, must be greater than 0, otherwise will panic
    /// * If `None`, there's no effective limit.
    ///
    /// # See also
    /// Truncation of Page Index statistics is controlled separately via
    /// [`WriterPropertiesBuilder::set_column_index_truncate_length`]
    ///
    /// [`Statistics`]: crate::file::statistics::Statistics
    pub fn set_statistics_truncate_length(mut self, max_length: Option<usize>) -> Self {
        if let Some(value) = max_length {
            assert!(value > 0, "Cannot have a 0 statistics truncate length. If you wish to disable min/max value truncation, set it to `None`.");
        }

        self.statistics_truncate_length = max_length;
        self
    }

    /// Should the writer coerce types to parquet native types (defaults to `false` via
    /// [`DEFAULT_COERCE_TYPES`]).
    ///
    /// Leaving this option the default `false` will ensure the exact same data
    /// written to parquet using this library will be read.
    ///
    /// Setting this option to `true` will result in parquet files that can be
    /// read by more readers, but potentially lose information in the process.
    ///
    /// * Types such as [`DataType::Date64`], which have no direct corresponding
    ///   Parquet type, may be stored with lower precision.
    ///
    /// * The internal field names of `List` and `Map` types will be renamed if
    ///   necessary to match what is required by the newest Parquet specification.
    ///
    /// See [`ArrowToParquetSchemaConverter::with_coerce_types`] for more details
    ///
    /// [`DataType::Date64`]: arrow_schema::DataType::Date64
    /// [`ArrowToParquetSchemaConverter::with_coerce_types`]: crate::arrow::ArrowSchemaConverter::with_coerce_types
    pub fn set_coerce_types(mut self, coerce_types: bool) -> Self {
        self.coerce_types = coerce_types;
        self
    }

    /// Sets FileEncryptionProperties (defaults to `None`)
    #[cfg(feature = "encryption")]
    pub fn with_file_encryption_properties(
        mut self,
        file_encryption_properties: FileEncryptionProperties,
    ) -> Self {
        self.file_encryption_properties = Some(file_encryption_properties);
        self
    }

    // ----------------------------------------------------------------------
    // Setters for any column (global)

    /// Sets default encoding for all columns.
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for all
    /// columns. In case when dictionary is enabled for any column, this value is
    /// considered to be a fallback encoding for that column.
    ///
    /// # Panics
    ///
    /// if dictionary encoding is specified, regardless of dictionary
    /// encoding flag being set.
    pub fn set_encoding(mut self, value: Encoding) -> Self {
        self.default_column_properties.set_encoding(value);
        self
    }

    /// Sets default compression codec for all columns (default to [`UNCOMPRESSED`] via
    /// [`DEFAULT_COMPRESSION`]).
    ///
    /// [`UNCOMPRESSED`]: Compression::UNCOMPRESSED
    pub fn set_compression(mut self, value: Compression) -> Self {
        self.default_column_properties.set_compression(value);
        self
    }

    /// Sets default flag to enable/disable dictionary encoding for all columns (defaults to `true`
    /// via [`DEFAULT_DICTIONARY_ENABLED`]).
    ///
    /// Use this method to set dictionary encoding, instead of explicitly specifying
    /// encoding in `set_encoding` method.
    pub fn set_dictionary_enabled(mut self, value: bool) -> Self {
        self.default_column_properties.set_dictionary_enabled(value);
        self
    }

    /// Sets default statistics level for all columns (defaults to [`Page`] via
    /// [`DEFAULT_STATISTICS_ENABLED`]).
    ///
    /// [`Page`]: EnabledStatistics::Page
    pub fn set_statistics_enabled(mut self, value: EnabledStatistics) -> Self {
        self.default_column_properties.set_statistics_enabled(value);
        self
    }

    /// Sets default max statistics size for all columns (defaults to `4096` via
    /// [`DEFAULT_MAX_STATISTICS_SIZE`]).
    ///
    /// Applicable only if statistics are enabled.
    #[deprecated(since = "54.0.0", note = "Unused; will be removed in 56.0.0")]
    pub fn set_max_statistics_size(mut self, value: usize) -> Self {
        #[allow(deprecated)]
        self.default_column_properties
            .set_max_statistics_size(value);
        self
    }

    /// Sets if bloom filter should be written for all columns (defaults to `false`).
    ///
    /// # Notes
    ///
    /// * If the bloom filter is enabled previously then it is a no-op.
    ///
    /// * If the bloom filter is not enabled, default values for ndv and fpp
    ///   value are used used. See [`set_bloom_filter_ndv`] and
    ///   [`set_bloom_filter_fpp`] to further adjust the ndv and fpp.
    ///
    /// [`set_bloom_filter_ndv`]: Self::set_bloom_filter_ndv
    /// [`set_bloom_filter_fpp`]: Self::set_bloom_filter_fpp
    pub fn set_bloom_filter_enabled(mut self, value: bool) -> Self {
        self.default_column_properties
            .set_bloom_filter_enabled(value);
        self
    }

    /// Sets the default target bloom filter false positive probability (fpp)
    /// for all columns (defaults to `0.05` via [`DEFAULT_BLOOM_FILTER_FPP`]).
    ///
    /// Implicitly enables bloom writing, as if [`set_bloom_filter_enabled`] had
    /// been called.
    ///
    /// [`set_bloom_filter_enabled`]: Self::set_bloom_filter_enabled
    pub fn set_bloom_filter_fpp(mut self, value: f64) -> Self {
        self.default_column_properties.set_bloom_filter_fpp(value);
        self
    }

    /// Sets default number of distinct values (ndv) for bloom filter for all
    /// columns (defaults to `1_000_000` via [`DEFAULT_BLOOM_FILTER_NDV`]).
    ///
    /// Implicitly enables bloom writing, as if [`set_bloom_filter_enabled`] had
    /// been called.
    ///
    /// [`set_bloom_filter_enabled`]: Self::set_bloom_filter_enabled
    pub fn set_bloom_filter_ndv(mut self, value: u64) -> Self {
        self.default_column_properties.set_bloom_filter_ndv(value);
        self
    }

    // ----------------------------------------------------------------------
    // Setters for a specific column

    /// Helper method to get existing or new mutable reference of column properties.
    #[inline]
    fn get_mut_props(&mut self, col: ColumnPath) -> &mut ColumnProperties {
        self.column_properties.entry(col).or_default()
    }

    /// Sets encoding for a specific column.
    ///
    /// Takes precedence over [`Self::set_encoding`].
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for this
    /// column. In case when dictionary is enabled for this column, either through
    /// global defaults or explicitly, this value is considered to be a fallback
    /// encoding for this column.
    ///
    /// # Panics
    /// If user tries to set dictionary encoding here, regardless of dictionary
    /// encoding flag being set.
    pub fn set_column_encoding(mut self, col: ColumnPath, value: Encoding) -> Self {
        self.get_mut_props(col).set_encoding(value);
        self
    }

    /// Sets compression codec for a specific column.
    ///
    /// Takes precedence over [`Self::set_compression`].
    pub fn set_column_compression(mut self, col: ColumnPath, value: Compression) -> Self {
        self.get_mut_props(col).set_compression(value);
        self
    }

    /// Sets flag to enable/disable dictionary encoding for a specific column.
    ///
    /// Takes precedence over [`Self::set_dictionary_enabled`].
    pub fn set_column_dictionary_enabled(mut self, col: ColumnPath, value: bool) -> Self {
        self.get_mut_props(col).set_dictionary_enabled(value);
        self
    }

    /// Sets dictionary page size limit for a specific column.
    ///
    /// Takes precedence over [`Self::set_dictionary_page_size_limit`].
    pub fn set_column_dictionary_page_size_limit(mut self, col: ColumnPath, value: usize) -> Self {
        self.get_mut_props(col)
            .set_dictionary_page_size_limit(value);
        self
    }

    /// Sets statistics level for a specific column
    ///
    /// Takes precedence over [`Self::set_statistics_enabled`].
    pub fn set_column_statistics_enabled(
        mut self,
        col: ColumnPath,
        value: EnabledStatistics,
    ) -> Self {
        self.get_mut_props(col).set_statistics_enabled(value);
        self
    }

    /// Sets max size for statistics for a specific column.
    ///
    /// Takes precedence over [`Self::set_max_statistics_size`].
    #[deprecated(since = "54.0.0", note = "Unused; will be removed in 56.0.0")]
    pub fn set_column_max_statistics_size(mut self, col: ColumnPath, value: usize) -> Self {
        #[allow(deprecated)]
        self.get_mut_props(col).set_max_statistics_size(value);
        self
    }

    /// Sets whether a bloom filter should be written for a specific column.
    ///
    /// Takes precedence over [`Self::set_bloom_filter_enabled`].
    pub fn set_column_bloom_filter_enabled(mut self, col: ColumnPath, value: bool) -> Self {
        self.get_mut_props(col).set_bloom_filter_enabled(value);
        self
    }

    /// Sets the false positive probability for bloom filter for a specific column.
    ///
    /// Takes precedence over [`Self::set_bloom_filter_fpp`].
    pub fn set_column_bloom_filter_fpp(mut self, col: ColumnPath, value: f64) -> Self {
        self.get_mut_props(col).set_bloom_filter_fpp(value);
        self
    }

    /// Sets the number of distinct values for bloom filter for a specific column.
    ///
    /// Takes precedence over [`Self::set_bloom_filter_ndv`].
    pub fn set_column_bloom_filter_ndv(mut self, col: ColumnPath, value: u64) -> Self {
        self.get_mut_props(col).set_bloom_filter_ndv(value);
        self
    }
}

/// Controls the level of statistics to be computed by the writer and stored in
/// the parquet file.
///
/// Enabling statistics makes the resulting Parquet file larger and requires
/// more time to read the parquet footer.
///
/// Statistics can be used to improve query performance by pruning row groups
/// and pages during query execution if the query engine supports evaluating the
/// predicate using the statistics.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EnabledStatistics {
    /// Compute no statistics.
    None,
    /// Compute column chunk-level statistics but not page-level.
    ///
    /// Setting this option will store one set of statistics for each relevant
    /// column for each row group. The more row groups written, the more
    /// statistics will be stored.
    Chunk,
    /// Compute page-level and column chunk-level statistics.
    ///
    /// Setting this option will store one set of statistics for each relevant
    /// column for each page and row group. The more row groups and the more
    /// pages written, the more statistics will be stored.
    Page,
}

impl FromStr for EnabledStatistics {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NONE" | "none" => Ok(EnabledStatistics::None),
            "CHUNK" | "chunk" => Ok(EnabledStatistics::Chunk),
            "PAGE" | "page" => Ok(EnabledStatistics::Page),
            _ => Err(format!("Invalid statistics arg: {s}")),
        }
    }
}

impl Default for EnabledStatistics {
    fn default() -> Self {
        DEFAULT_STATISTICS_ENABLED
    }
}

/// Controls the bloom filter to be computed by the writer.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilterProperties {
    /// False positive probability. This should be always between 0 and 1 exclusive. Defaults to [`DEFAULT_BLOOM_FILTER_FPP`].
    ///
    /// You should set this value by calling [`WriterPropertiesBuilder::set_bloom_filter_fpp`].
    ///
    /// The bloom filter data structure is a trade of between disk and memory space versus fpp, the
    /// smaller the fpp, the more memory and disk space is required, thus setting it to a reasonable value
    /// e.g. 0.1, 0.05, or 0.001 is recommended.
    ///
    /// Setting to a very small number diminishes the value of the filter itself, as the bitset size is
    /// even larger than just storing the whole value. You are also expected to set `ndv` if it can
    /// be known in advance to greatly reduce space usage.
    pub fpp: f64,
    /// Number of distinct values, should be non-negative to be meaningful. Defaults to [`DEFAULT_BLOOM_FILTER_NDV`].
    ///
    /// You should set this value by calling [`WriterPropertiesBuilder::set_bloom_filter_ndv`].
    ///
    /// Usage of bloom filter is most beneficial for columns with large cardinality, so a good heuristic
    /// is to set ndv to the number of rows. However, it can reduce disk size if you know in advance a smaller
    /// number of distinct values. For very small ndv value it is probably not worth it to use bloom filter
    /// anyway.
    ///
    /// Increasing this value (without increasing fpp) will result in an increase in disk or memory size.
    pub ndv: u64,
}

impl Default for BloomFilterProperties {
    fn default() -> Self {
        BloomFilterProperties {
            fpp: DEFAULT_BLOOM_FILTER_FPP,
            ndv: DEFAULT_BLOOM_FILTER_NDV,
        }
    }
}

/// Container for column properties that can be changed as part of writer.
///
/// If a field is `None`, it means that no specific value has been set for this column,
/// so some subsequent or default value must be used.
#[derive(Debug, Clone, Default, PartialEq)]
struct ColumnProperties {
    encoding: Option<Encoding>,
    codec: Option<Compression>,
    dictionary_page_size_limit: Option<usize>,
    dictionary_enabled: Option<bool>,
    statistics_enabled: Option<EnabledStatistics>,
    #[deprecated(since = "54.0.0", note = "Unused; will be removed in 56.0.0")]
    max_statistics_size: Option<usize>,
    /// bloom filter related properties
    bloom_filter_properties: Option<BloomFilterProperties>,
}

impl ColumnProperties {
    /// Sets encoding for this column.
    ///
    /// If dictionary is not enabled, this is treated as a primary encoding for a column.
    /// In case when dictionary is enabled for a column, this value is considered to
    /// be a fallback encoding.
    ///
    /// Panics if user tries to set dictionary encoding here, regardless of dictionary
    /// encoding flag being set. Use `set_dictionary_enabled` method to enable dictionary
    /// for a column.
    fn set_encoding(&mut self, value: Encoding) {
        if value == Encoding::PLAIN_DICTIONARY || value == Encoding::RLE_DICTIONARY {
            panic!("Dictionary encoding can not be used as fallback encoding");
        }
        self.encoding = Some(value);
    }

    /// Sets compression codec for this column.
    fn set_compression(&mut self, value: Compression) {
        self.codec = Some(value);
    }

    /// Sets whether dictionary encoding is enabled for this column.
    fn set_dictionary_enabled(&mut self, enabled: bool) {
        self.dictionary_enabled = Some(enabled);
    }

    /// Sets dictionary page size limit for this column.
    fn set_dictionary_page_size_limit(&mut self, value: usize) {
        self.dictionary_page_size_limit = Some(value);
    }

    /// Sets the statistics level for this column.
    fn set_statistics_enabled(&mut self, enabled: EnabledStatistics) {
        self.statistics_enabled = Some(enabled);
    }

    /// Sets max size for statistics for this column.
    #[deprecated(since = "54.0.0", note = "Unused; will be removed in 56.0.0")]
    #[allow(deprecated)]
    fn set_max_statistics_size(&mut self, value: usize) {
        self.max_statistics_size = Some(value);
    }

    /// If `value` is `true`, sets bloom filter properties to default values if not previously set,
    /// otherwise it is a no-op.
    /// If `value` is `false`, resets bloom filter properties to `None`.
    fn set_bloom_filter_enabled(&mut self, value: bool) {
        if value && self.bloom_filter_properties.is_none() {
            self.bloom_filter_properties = Some(Default::default())
        } else if !value {
            self.bloom_filter_properties = None
        }
    }

    /// Sets the false positive probability for bloom filter for this column, and implicitly enables
    /// bloom filter if not previously enabled.
    ///
    /// # Panics
    ///
    /// Panics if the `value` is not between 0 and 1 exclusive
    fn set_bloom_filter_fpp(&mut self, value: f64) {
        assert!(
            value > 0. && value < 1.0,
            "fpp must be between 0 and 1 exclusive, got {value}"
        );

        self.bloom_filter_properties
            .get_or_insert_with(Default::default)
            .fpp = value;
    }

    /// Sets the number of distinct (unique) values for bloom filter for this column, and implicitly
    /// enables bloom filter if not previously enabled.
    fn set_bloom_filter_ndv(&mut self, value: u64) {
        self.bloom_filter_properties
            .get_or_insert_with(Default::default)
            .ndv = value;
    }

    /// Returns optional encoding for this column.
    fn encoding(&self) -> Option<Encoding> {
        self.encoding
    }

    /// Returns optional compression codec for this column.
    fn compression(&self) -> Option<Compression> {
        self.codec
    }

    /// Returns `Some(true)` if dictionary encoding is enabled for this column, if
    /// disabled then returns `Some(false)`. If result is `None`, then no setting has
    /// been provided.
    fn dictionary_enabled(&self) -> Option<bool> {
        self.dictionary_enabled
    }

    /// Returns optional dictionary page size limit for this column.
    fn dictionary_page_size_limit(&self) -> Option<usize> {
        self.dictionary_page_size_limit
    }

    /// Returns optional statistics level requested for this column. If result is `None`,
    /// then no setting has been provided.
    fn statistics_enabled(&self) -> Option<EnabledStatistics> {
        self.statistics_enabled
    }

    /// Returns optional max size in bytes for statistics.
    #[deprecated(since = "54.0.0", note = "Unused; will be removed in 56.0.0")]
    fn max_statistics_size(&self) -> Option<usize> {
        #[allow(deprecated)]
        self.max_statistics_size
    }

    /// Returns the bloom filter properties, or `None` if not enabled
    fn bloom_filter_properties(&self) -> Option<&BloomFilterProperties> {
        self.bloom_filter_properties.as_ref()
    }
}

/// Reference counted reader properties.
pub type ReaderPropertiesPtr = Arc<ReaderProperties>;

const DEFAULT_READ_BLOOM_FILTER: bool = false;

/// Configuration settings for reading parquet files.
///
/// All properties are immutable and `Send` + `Sync`.
/// Use [`ReaderPropertiesBuilder`] to assemble these properties.
///
/// # Example
///
/// ```rust
/// use parquet::file::properties::ReaderProperties;
///
/// // Create properties with default configuration.
/// let props = ReaderProperties::builder().build();
///
/// // Use properties builder to set certain options and assemble the configuration.
/// let props = ReaderProperties::builder()
///     .set_backward_compatible_lz4(false)
///     .build();
/// ```
pub struct ReaderProperties {
    codec_options: CodecOptions,
    read_bloom_filter: bool,
}

impl ReaderProperties {
    /// Returns builder for reader properties with default values.
    pub fn builder() -> ReaderPropertiesBuilder {
        ReaderPropertiesBuilder::with_defaults()
    }

    /// Returns codec options.
    pub(crate) fn codec_options(&self) -> &CodecOptions {
        &self.codec_options
    }

    /// Returns whether to read bloom filter
    pub(crate) fn read_bloom_filter(&self) -> bool {
        self.read_bloom_filter
    }
}

/// Builder for parquet file reader configuration. See example on
/// [`ReaderProperties`]
pub struct ReaderPropertiesBuilder {
    codec_options_builder: CodecOptionsBuilder,
    read_bloom_filter: Option<bool>,
}

/// Reader properties builder.
impl ReaderPropertiesBuilder {
    /// Returns default state of the builder.
    fn with_defaults() -> Self {
        Self {
            codec_options_builder: CodecOptionsBuilder::default(),
            read_bloom_filter: None,
        }
    }

    /// Finalizes the configuration and returns immutable reader properties struct.
    pub fn build(self) -> ReaderProperties {
        ReaderProperties {
            codec_options: self.codec_options_builder.build(),
            read_bloom_filter: self.read_bloom_filter.unwrap_or(DEFAULT_READ_BLOOM_FILTER),
        }
    }

    /// Enable/disable backward compatible LZ4.
    ///
    /// If backward compatible LZ4 is enable, on LZ4_HADOOP error it will fallback
    /// to the older versions LZ4 algorithms. That is LZ4_FRAME, for backward compatibility
    /// with files generated by older versions of this library, and LZ4_RAW, for backward
    /// compatibility with files generated by older versions of parquet-cpp.
    ///
    /// If backward compatible LZ4 is disabled, on LZ4_HADOOP error it will return the error.
    pub fn set_backward_compatible_lz4(mut self, value: bool) -> Self {
        self.codec_options_builder = self
            .codec_options_builder
            .set_backward_compatible_lz4(value);
        self
    }

    /// Enable/disable reading bloom filter
    ///
    /// If reading bloom filter is enabled, bloom filter will be read from the file.
    /// If reading bloom filter is disabled, bloom filter will not be read from the file.
    ///
    /// By default bloom filter is set to be read.
    pub fn set_read_bloom_filter(mut self, value: bool) -> Self {
        self.read_bloom_filter = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_version() {
        assert_eq!(WriterVersion::PARQUET_1_0.as_num(), 1);
        assert_eq!(WriterVersion::PARQUET_2_0.as_num(), 2);
    }

    #[test]
    fn test_writer_properties_default_settings() {
        let props = WriterProperties::default();
        assert_eq!(props.data_page_size_limit(), DEFAULT_PAGE_SIZE);
        assert_eq!(
            props.dictionary_page_size_limit(),
            DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
        );
        assert_eq!(props.write_batch_size(), DEFAULT_WRITE_BATCH_SIZE);
        assert_eq!(props.max_row_group_size(), DEFAULT_MAX_ROW_GROUP_SIZE);
        assert_eq!(props.bloom_filter_position(), DEFAULT_BLOOM_FILTER_POSITION);
        assert_eq!(props.writer_version(), DEFAULT_WRITER_VERSION);
        assert_eq!(props.created_by(), DEFAULT_CREATED_BY);
        assert_eq!(props.key_value_metadata(), None);
        assert_eq!(props.encoding(&ColumnPath::from("col")), None);
        assert_eq!(
            props.compression(&ColumnPath::from("col")),
            DEFAULT_COMPRESSION
        );
        assert_eq!(
            props.dictionary_enabled(&ColumnPath::from("col")),
            DEFAULT_DICTIONARY_ENABLED
        );
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("col")),
            DEFAULT_STATISTICS_ENABLED
        );
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("col"))
            .is_none());
    }

    #[test]
    fn test_writer_properties_dictionary_encoding() {
        // dictionary encoding is not configurable, and it should be the same for both
        // writer version 1 and 2.
        for version in &[WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            let props = WriterProperties::builder()
                .set_writer_version(*version)
                .build();
            assert_eq!(props.dictionary_page_encoding(), Encoding::PLAIN);
            assert_eq!(
                props.dictionary_data_page_encoding(),
                Encoding::RLE_DICTIONARY
            );
        }
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_plain_dictionary_is_fallback() {
        // Should panic when user specifies dictionary encoding as fallback encoding.
        WriterProperties::builder()
            .set_encoding(Encoding::PLAIN_DICTIONARY)
            .build();
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_rle_dictionary_is_fallback() {
        // Should panic when user specifies dictionary encoding as fallback encoding.
        WriterProperties::builder()
            .set_encoding(Encoding::RLE_DICTIONARY)
            .build();
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_dictionary_is_enabled() {
        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE_DICTIONARY)
            .build();
    }

    #[test]
    #[should_panic(expected = "Dictionary encoding can not be used as fallback encoding")]
    fn test_writer_properties_panic_when_dictionary_is_disabled() {
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE_DICTIONARY)
            .build();
    }

    #[test]
    fn test_writer_properties_builder() {
        let props = WriterProperties::builder()
            // file settings
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_data_page_size_limit(10)
            .set_dictionary_page_size_limit(20)
            .set_write_batch_size(30)
            .set_max_row_group_size(40)
            .set_created_by("default".to_owned())
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "key".to_string(),
                "value".to_string(),
            )]))
            // global column settings
            .set_encoding(Encoding::DELTA_BINARY_PACKED)
            .set_compression(Compression::GZIP(Default::default()))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            // specific column settings
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE)
            .set_column_compression(ColumnPath::from("col"), Compression::SNAPPY)
            .set_column_dictionary_enabled(ColumnPath::from("col"), true)
            .set_column_statistics_enabled(ColumnPath::from("col"), EnabledStatistics::Chunk)
            .set_column_bloom_filter_enabled(ColumnPath::from("col"), true)
            .set_column_bloom_filter_ndv(ColumnPath::from("col"), 100_u64)
            .set_column_bloom_filter_fpp(ColumnPath::from("col"), 0.1)
            .build();

        assert_eq!(props.writer_version(), WriterVersion::PARQUET_2_0);
        assert_eq!(props.data_page_size_limit(), 10);
        assert_eq!(props.dictionary_page_size_limit(), 20);
        assert_eq!(props.write_batch_size(), 30);
        assert_eq!(props.max_row_group_size(), 40);
        assert_eq!(props.created_by(), "default");
        assert_eq!(
            props.key_value_metadata(),
            Some(&vec![
                KeyValue::new("key".to_string(), "value".to_string(),)
            ])
        );

        assert_eq!(
            props.encoding(&ColumnPath::from("a")),
            Some(Encoding::DELTA_BINARY_PACKED)
        );
        assert_eq!(
            props.compression(&ColumnPath::from("a")),
            Compression::GZIP(Default::default())
        );
        assert!(!props.dictionary_enabled(&ColumnPath::from("a")));
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("a")),
            EnabledStatistics::None
        );

        assert_eq!(
            props.encoding(&ColumnPath::from("col")),
            Some(Encoding::RLE)
        );
        assert_eq!(
            props.compression(&ColumnPath::from("col")),
            Compression::SNAPPY
        );
        assert!(props.dictionary_enabled(&ColumnPath::from("col")));
        assert_eq!(
            props.statistics_enabled(&ColumnPath::from("col")),
            EnabledStatistics::Chunk
        );
        assert_eq!(
            props.bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties { fpp: 0.1, ndv: 100 })
        );
    }

    #[test]
    fn test_writer_properties_builder_partial_defaults() {
        let props = WriterProperties::builder()
            .set_encoding(Encoding::DELTA_BINARY_PACKED)
            .set_compression(Compression::GZIP(Default::default()))
            .set_bloom_filter_enabled(true)
            .set_column_encoding(ColumnPath::from("col"), Encoding::RLE)
            .build();

        assert_eq!(
            props.encoding(&ColumnPath::from("col")),
            Some(Encoding::RLE)
        );
        assert_eq!(
            props.compression(&ColumnPath::from("col")),
            Compression::GZIP(Default::default())
        );
        assert_eq!(
            props.dictionary_enabled(&ColumnPath::from("col")),
            DEFAULT_DICTIONARY_ENABLED
        );
        assert_eq!(
            props.bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties {
                fpp: 0.05,
                ndv: 1_000_000_u64
            })
        );
    }

    #[test]
    fn test_writer_properties_bloom_filter_ndv_fpp_set() {
        assert_eq!(
            WriterProperties::builder()
                .build()
                .bloom_filter_properties(&ColumnPath::from("col")),
            None
        );
        assert_eq!(
            WriterProperties::builder()
                .set_bloom_filter_ndv(100)
                .build()
                .bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties {
                fpp: 0.05,
                ndv: 100
            })
        );
        assert_eq!(
            WriterProperties::builder()
                .set_bloom_filter_fpp(0.1)
                .build()
                .bloom_filter_properties(&ColumnPath::from("col")),
            Some(&BloomFilterProperties {
                fpp: 0.1,
                ndv: 1_000_000_u64
            })
        );
    }

    #[test]
    fn test_writer_properties_column_dictionary_page_size_limit() {
        let props = WriterProperties::builder()
            .set_dictionary_page_size_limit(100)
            .set_column_dictionary_page_size_limit(ColumnPath::from("col"), 10)
            .build();

        assert_eq!(props.dictionary_page_size_limit(), 100);
        assert_eq!(
            props.column_dictionary_page_size_limit(&ColumnPath::from("col")),
            10
        );
        assert_eq!(
            props.column_dictionary_page_size_limit(&ColumnPath::from("other")),
            100
        );
    }

    #[test]
    fn test_reader_properties_default_settings() {
        let props = ReaderProperties::builder().build();

        let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(true)
            .build();

        assert_eq!(props.codec_options(), &codec_options);
        assert!(!props.read_bloom_filter());
    }

    #[test]
    fn test_reader_properties_builder() {
        let props = ReaderProperties::builder()
            .set_backward_compatible_lz4(false)
            .build();

        let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();

        assert_eq!(props.codec_options(), &codec_options);
    }

    #[test]
    fn test_parse_writerversion() {
        let mut writer_version = "PARQUET_1_0".parse::<WriterVersion>().unwrap();
        assert_eq!(writer_version, WriterVersion::PARQUET_1_0);
        writer_version = "PARQUET_2_0".parse::<WriterVersion>().unwrap();
        assert_eq!(writer_version, WriterVersion::PARQUET_2_0);

        // test lowercase
        writer_version = "parquet_1_0".parse::<WriterVersion>().unwrap();
        assert_eq!(writer_version, WriterVersion::PARQUET_1_0);

        // test invalid version
        match "PARQUET_-1_0".parse::<WriterVersion>() {
            Ok(_) => panic!("Should not be able to parse PARQUET_-1_0"),
            Err(e) => {
                assert_eq!(e, "Invalid writer version: PARQUET_-1_0");
            }
        }
    }

    #[test]
    fn test_parse_enabledstatistics() {
        let mut enabled_statistics = "NONE".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::None);
        enabled_statistics = "CHUNK".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::Chunk);
        enabled_statistics = "PAGE".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::Page);

        // test lowercase
        enabled_statistics = "none".parse::<EnabledStatistics>().unwrap();
        assert_eq!(enabled_statistics, EnabledStatistics::None);

        //test invalid statistics
        match "ChunkAndPage".parse::<EnabledStatistics>() {
            Ok(_) => panic!("Should not be able to parse ChunkAndPage"),
            Err(e) => {
                assert_eq!(e, "Invalid statistics arg: ChunkAndPage");
            }
        }
    }
}
