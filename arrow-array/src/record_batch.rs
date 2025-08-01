// This file contains both Apache Software Foundation (ASF) licensed code as
// well as Synnada, Inc. extensions. Changes that constitute Synnada, Inc.
// extensions are available in the SYNNADA-CONTRIBUTIONS.txt file. Synnada, Inc.
// claims copyright only for Synnada, Inc. extensions. The license notice
// applicable to non-Synnada sections of the file is given below.
// --
//
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

//! A two-dimensional batch of column-oriented data with a defined
//! [schema](arrow_schema::Schema).

use crate::cast::AsArray;
use crate::{new_empty_array, Array, ArrayRef, StructArray};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaBuilder, SchemaRef};
use std::ops::Index;
use std::sync::Arc;

/// THIS CONSTANT IS ARAS ONLY
pub const NORMAL_RECORD_BATCH: u8 = 0;
/// THIS CONSTANT IS ARAS ONLY
///
/// Whether the watermark is [`SOURCE_GENERATED_WATERMARK`] or
/// [`INTERMEDIATE_NODE_GENERATED_WATERMARK`] is not be important
/// for the watermark algorithms. They are seperated because of
/// testing purposes. Once the watermark infrastructure is solid
/// and complete, then we will unify them.
pub const SOURCE_GENERATED_WATERMARK: u8 = 1;
/// THIS CONSTANT IS ARAS ONLY
///
/// Whether the watermark is [`SOURCE_GENERATED_WATERMARK`] or
/// [`INTERMEDIATE_NODE_GENERATED_WATERMARK`] is not be important
/// for the watermark algorithms. They are seperated because of
/// testing purposes. Once the watermark infrastructure is solid
/// and complete, then we will unify them.
pub const INTERMEDIATE_NODE_GENERATED_WATERMARK: u8 = 2;
/// THIS CONSTANT IS ARAS ONLY
pub const CHECKPOINT_MESSAGE: u8 = 3;

/// Trait for types that can read `RecordBatch`'s.
///
/// To create from an iterator, see [RecordBatchIterator].
pub trait RecordBatchReader: Iterator<Item = Result<RecordBatch, ArrowError>> {
    /// Returns the schema of this `RecordBatchReader`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

impl<R: RecordBatchReader + ?Sized> RecordBatchReader for Box<R> {
    fn schema(&self) -> SchemaRef {
        self.as_ref().schema()
    }
}

/// Trait for types that can write `RecordBatch`'s.
pub trait RecordBatchWriter {
    /// Write a single batch to the writer.
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError>;

    /// Write footer or termination data, then mark the writer as done.
    fn close(self) -> Result<(), ArrowError>;
}

/// Creates an array from a literal slice of values,
/// suitable for rapid testing and development.
///
/// Example:
///
/// ```rust
///
/// use arrow_array::create_array;
///
/// let array = create_array!(Int32, [1, 2, 3, 4, 5]);
/// let array = create_array!(Utf8, [Some("a"), Some("b"), None, Some("e")]);
/// ```
/// Support for limited data types is available. The macro will return a compile error if an unsupported data type is used.
/// Presently supported data types are:
/// - `Boolean`, `Null`
/// - `Decimal128`, `Decimal256`
/// - `Float16`, `Float32`, `Float64`
/// - `Int8`, `Int16`, `Int32`, `Int64`
/// - `UInt8`, `UInt16`, `UInt32`, `UInt64`
/// - `IntervalDayTime`, `IntervalYearMonth`
/// - `Second`, `Millisecond`, `Microsecond`, `Nanosecond`
/// - `Second32`, `Millisecond32`, `Microsecond64`, `Nanosecond64`
/// - `DurationSecond`, `DurationMillisecond`, `DurationMicrosecond`, `DurationNanosecond`
/// - `TimestampSecond`, `TimestampMillisecond`, `TimestampMicrosecond`, `TimestampNanosecond`
/// - `Utf8`, `Utf8View`, `LargeUtf8`, `Binary`, `LargeBinary`
#[macro_export]
macro_rules! create_array {
    // `@from` is used for those types that have a common method `<type>::from`
    (@from Boolean) => { $crate::BooleanArray };
    (@from Int8) => { $crate::Int8Array };
    (@from Int16) => { $crate::Int16Array };
    (@from Int32) => { $crate::Int32Array };
    (@from Int64) => { $crate::Int64Array };
    (@from UInt8) => { $crate::UInt8Array };
    (@from UInt16) => { $crate::UInt16Array };
    (@from UInt32) => { $crate::UInt32Array };
    (@from UInt64) => { $crate::UInt64Array };
    (@from Float16) => { $crate::Float16Array };
    (@from Float32) => { $crate::Float32Array };
    (@from Float64) => { $crate::Float64Array };
    (@from Utf8) => { $crate::StringArray };
    (@from Utf8View) => { $crate::StringViewArray };
    (@from LargeUtf8) => { $crate::LargeStringArray };
    (@from IntervalDayTime) => { $crate::IntervalDayTimeArray };
    (@from IntervalYearMonth) => { $crate::IntervalYearMonthArray };
    (@from Second) => { $crate::TimestampSecondArray };
    (@from Millisecond) => { $crate::TimestampMillisecondArray };
    (@from Microsecond) => { $crate::TimestampMicrosecondArray };
    (@from Nanosecond) => { $crate::TimestampNanosecondArray };
    (@from Second32) => { $crate::Time32SecondArray };
    (@from Millisecond32) => { $crate::Time32MillisecondArray };
    (@from Microsecond64) => { $crate::Time64MicrosecondArray };
    (@from Nanosecond64) => { $crate::Time64Nanosecond64Array };
    (@from DurationSecond) => { $crate::DurationSecondArray };
    (@from DurationMillisecond) => { $crate::DurationMillisecondArray };
    (@from DurationMicrosecond) => { $crate::DurationMicrosecondArray };
    (@from DurationNanosecond) => { $crate::DurationNanosecondArray };
    (@from Decimal128) => { $crate::Decimal128Array };
    (@from Decimal256) => { $crate::Decimal256Array };
    (@from TimestampSecond) => { $crate::TimestampSecondArray };
    (@from TimestampMillisecond) => { $crate::TimestampMillisecondArray };
    (@from TimestampMicrosecond) => { $crate::TimestampMicrosecondArray };
    (@from TimestampNanosecond) => { $crate::TimestampNanosecondArray };

    (@from $ty: ident) => {
        compile_error!(concat!("Unsupported data type: ", stringify!($ty)))
    };

    (Null, $size: expr) => {
        std::sync::Arc::new($crate::NullArray::new($size))
    };

    (Binary, [$($values: expr),*]) => {
        std::sync::Arc::new($crate::BinaryArray::from_vec(vec![$($values),*]))
    };

    (LargeBinary, [$($values: expr),*]) => {
        std::sync::Arc::new($crate::LargeBinaryArray::from_vec(vec![$($values),*]))
    };

    ($ty: tt, [$($values: expr),*]) => {
        std::sync::Arc::new(<$crate::create_array!(@from $ty)>::from(vec![$($values),*]))
    };
}

/// Creates a record batch from literal slice of values, suitable for rapid
/// testing and development.
///
/// Example:
///
/// ```rust
/// use arrow_array::record_batch;
/// use arrow_schema;
///
/// let batch = record_batch!(
///     ("a", Int32, [1, 2, 3]),
///     ("b", Float64, [Some(4.0), None, Some(5.0)]),
///     ("c", Utf8, ["alpha", "beta", "gamma"])
/// );
/// ```
/// Due to limitation of [`create_array!`] macro, support for limited data types is available.
#[macro_export]
macro_rules! record_batch {
    ($(($name: expr, $type: ident, [$($values: expr),*])),*) => {
        {
            let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
                $(
                    arrow_schema::Field::new($name, arrow_schema::DataType::$type, true),
                )*
            ]));

            let batch = $crate::RecordBatch::try_new(
                schema,
                vec![$(
                    $crate::create_array!($type, [$($values),*]),
                )*]
            );

            batch
        }
    }
}

/// A two-dimensional batch of column-oriented data with a defined
/// [schema](arrow_schema::Schema).
///
/// A `RecordBatch` is a two-dimensional dataset of a number of
/// contiguous arrays, each the same length.
/// A record batch has a schema which must match its arrays’
/// datatypes.
///
/// Record batches are a convenient unit of work for various
/// serialization and computation functions, possibly incremental.
///
/// Use the [`record_batch!`] macro to create a [`RecordBatch`] from
/// literal slice of values, useful for rapid prototyping and testing.
///
/// Example:
/// ```rust
/// use arrow_array::record_batch;
/// let batch = record_batch!(
///     ("a", Int32, [1, 2, 3]),
///     ("b", Float64, [Some(4.0), None, Some(5.0)]),
///     ("c", Utf8, ["alpha", "beta", "gamma"])
/// );
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    schema: SchemaRef,
    columns: Vec<Arc<dyn Array>>,

    /// The number of rows in this RecordBatch
    ///
    /// This is stored separately from the columns to handle the case of no columns
    row_count: usize,

    /// THIS MEMBER IS ARAS ONLY
    ///
    /// This tag is used to store metadata flags for the record batch. This
    /// should include information that do not make a material change in the
    /// schema, but still needs to be carried with. Carrying such information
    /// as part of schema metadata results in "schema mismatch" issues, so
    /// this attribute should be used in such cases. Typical downstream use
    /// cases include specifying whether the record batch is a proper data
    /// batch, or a watermark batch, or a control message like a checkpoint
    /// barrier.
    ///
    /// The flags define the type of data or message represented by the record batch.
    /// The following values are currently defined:
    ///
    /// - `0`: Normal record batch data.
    /// - `1`: A source-generated watermark.
    /// - `2`: An intermediate node-generated watermark.
    /// - `3`: A checkpoint message.
    ///
    /// Additional flag values may be defined in the future to support new use cases.
    metadata_flags: u8,
}

impl RecordBatch {
    /// Creates a `RecordBatch` from a schema and columns.
    ///
    /// Expects the following:
    ///
    ///  * `!columns.is_empty()`
    ///  * `schema.fields.len() == columns.len()`
    ///  * `schema.fields[i].data_type() == columns[i].data_type()`
    ///  * `columns[i].len() == columns[j].len()`
    ///
    /// If the conditions are not met, an error is returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema),
    ///     vec![Arc::new(id_array)]
    /// ).unwrap();
    /// ```
    pub fn try_new(schema: SchemaRef, columns: Vec<ArrayRef>) -> Result<Self, ArrowError> {
        let options = RecordBatchOptions::new();
        Self::try_new_impl(schema, columns, &options)
    }

    /// THIS METHOD IS COMMON, MODIFIED BY ARAS
    ///
    /// Creates a `RecordBatch` from a schema and columns, without validation.
    ///
    /// See [`Self::try_new`] for the checked version.
    ///
    /// # Safety
    ///
    /// Expects the following:
    ///
    ///  * `schema.fields.len() == columns.len()`
    ///  * `schema.fields[i].data_type() == columns[i].data_type()`
    ///  * `columns[i].len() == row_count`
    ///
    /// Note: if the schema does not match the underlying data exactly, it can lead to undefined
    /// behavior, for example, via conversion to a `StructArray`, which in turn could lead
    /// to incorrect access.
    pub unsafe fn new_unchecked(
        schema: SchemaRef,
        columns: Vec<Arc<dyn Array>>,
        row_count: usize,
    ) -> Self {
        Self {
            schema,
            columns,
            row_count,
            metadata_flags: 0,
        }
    }

    /// THIS METHOD IS COMMON, MODIFIED BY ARAS
    ///
    /// Creates a `RecordBatch` from a schema and columns, with additional options,
    /// such as whether to strictly validate field names.
    ///
    /// See [`RecordBatch::try_new`] for the expected conditions.
    pub fn try_new_with_options(
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        options: &RecordBatchOptions,
    ) -> Result<Self, ArrowError> {
        Self::try_new_impl(schema, columns, options)
    }

    /// Creates a new empty [`RecordBatch`].
    pub fn new_empty(schema: SchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch {
            schema,
            columns,
            row_count: 0,
            metadata_flags: 0,
        }
    }

    /// THIS METHOD IS COMMON, MODIFIED BY ARAS
    ///
    /// Validate the schema and columns using [`RecordBatchOptions`]. Returns an error
    /// if any validation check fails, otherwise returns the created [`Self`]
    fn try_new_impl(
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        options: &RecordBatchOptions,
    ) -> Result<Self, ArrowError> {
        // check that number of fields in schema match column length
        if schema.fields().len() != columns.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "number of columns({}) must match number of fields({}) in schema",
                columns.len(),
                schema.fields().len(),
            )));
        }

        let row_count = options
            .row_count
            .or_else(|| columns.first().map(|col| col.len()))
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "must either specify a row count or at least one column".to_string(),
                )
            })?;

        for (c, f) in columns.iter().zip(&schema.fields) {
            if !f.is_nullable() && c.null_count() > 0 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Column '{}' is declared as non-nullable but contains null values",
                    f.name()
                )));
            }
        }

        // check that all columns have the same row count
        if columns.iter().any(|c| c.len() != row_count) {
            let err = match options.row_count {
                Some(_) => "all columns in a record batch must have the specified row count",
                None => "all columns in a record batch must have the same length",
            };
            return Err(ArrowError::InvalidArgumentError(err.to_string()));
        }

        // function for comparing column type and field type
        // return true if 2 types are not matched
        let type_not_match = if options.match_field_names {
            |(_, (col_type, field_type)): &(usize, (&DataType, &DataType))| col_type != field_type
        } else {
            |(_, (col_type, field_type)): &(usize, (&DataType, &DataType))| {
                !col_type.equals_datatype(field_type)
            }
        };

        // check that all columns match the schema
        let not_match = columns
            .iter()
            .zip(schema.fields().iter())
            .map(|(col, field)| (col.data_type(), field.data_type()))
            .enumerate()
            .find(type_not_match);

        if let Some((i, (col_type, field_type))) = not_match {
            return Err(ArrowError::InvalidArgumentError(format!(
                "column types must match schema types, expected {field_type:?} but found {col_type:?} at column index {i}")));
        }

        Ok(RecordBatch {
            schema,
            columns,
            row_count,
            metadata_flags: options.metadata_flags,
        })
    }

    /// Return the schema, columns and row count of this [`RecordBatch`]
    pub fn into_parts(self) -> (SchemaRef, Vec<ArrayRef>, usize) {
        (self.schema, self.columns, self.row_count)
    }

    /// THIS METHOD IS COMMON, MODIFIED BY ARAS
    ///
    /// Override the schema of this [`RecordBatch`]
    ///
    /// Returns an error if `schema` is not a superset of the current schema
    /// as determined by [`Schema::contains`]
    ///
    /// See also [`Self::schema_metadata_mut`].
    pub fn with_schema(self, schema: SchemaRef) -> Result<Self, ArrowError> {
        if !schema.contains(self.schema.as_ref()) {
            return Err(ArrowError::SchemaError(format!(
                "target schema is not superset of current schema target={schema} current={}",
                self.schema
            )));
        }

        Ok(Self {
            schema,
            columns: self.columns,
            row_count: self.row_count,
            metadata_flags: self.metadata_flags,
        })
    }

    /// Returns the [`Schema`] of the record batch.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns a reference to the [`Schema`] of the record batch.
    pub fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }

    /// Mutable access to the metadata of the schema.
    ///
    /// This allows you to modify [`Schema::metadata`] of [`Self::schema`] in a convenient and fast way.
    ///
    /// Note this will clone the entire underlying `Schema` object if it is currently shared
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{record_batch, RecordBatch};
    /// let mut batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
    /// // Initially, the metadata is empty
    /// assert!(batch.schema().metadata().get("key").is_none());
    /// // Insert a key-value pair into the metadata
    /// batch.schema_metadata_mut().insert("key".into(), "value".into());
    /// assert_eq!(batch.schema().metadata().get("key"), Some(&String::from("value")));
    /// ```    
    pub fn schema_metadata_mut(&mut self) -> &mut std::collections::HashMap<String, String> {
        let schema = Arc::make_mut(&mut self.schema);
        &mut schema.metadata
    }

    /// THIS METHOD IS COMMON, MODIFIED BY ARAS
    ///
    /// Projects the schema onto the specified columns
    pub fn project(&self, indices: &[usize]) -> Result<RecordBatch, ArrowError> {
        let projected_schema = self.schema.project(indices)?;
        let batch_fields = indices
            .iter()
            .map(|f| {
                self.columns.get(*f).cloned().ok_or_else(|| {
                    ArrowError::SchemaError(format!(
                        "project index {} out of bounds, max field {}",
                        f,
                        self.columns.len()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new_with_options(
            SchemaRef::new(projected_schema),
            batch_fields,
            &RecordBatchOptions {
                match_field_names: true,
                row_count: Some(self.row_count),
                metadata_flags: self.metadata_flags,
            },
        )
    }

    /// Normalize a semi-structured [`RecordBatch`] into a flat table.
    ///
    /// Nested [`Field`]s will generate names separated by `separator`, up to a depth of `max_level`
    /// (unlimited if `None`).
    ///
    /// e.g. given a [`RecordBatch`] with schema:
    ///
    /// ```text
    ///     "foo": StructArray<"bar": Utf8>
    /// ```
    ///
    /// A separator of `"."` would generate a batch with the schema:
    ///
    /// ```text
    ///     "foo.bar": Utf8
    /// ```
    ///
    /// Note that giving a depth of `Some(0)` to `max_level` is the same as passing in `None`;
    /// it will be treated as unlimited.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{ArrayRef, Int64Array, StringArray, StructArray, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Fields, Schema};
    /// #
    /// let animals: ArrayRef = Arc::new(StringArray::from(vec!["Parrot", ""]));
    /// let n_legs: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(4)]));
    ///
    /// let animals_field = Arc::new(Field::new("animals", DataType::Utf8, true));
    /// let n_legs_field = Arc::new(Field::new("n_legs", DataType::Int64, true));
    ///
    /// let a = Arc::new(StructArray::from(vec![
    ///     (animals_field.clone(), Arc::new(animals.clone()) as ArrayRef),
    ///     (n_legs_field.clone(), Arc::new(n_legs.clone()) as ArrayRef),
    /// ]));
    ///
    /// let schema = Schema::new(vec![
    ///     Field::new(
    ///         "a",
    ///         DataType::Struct(Fields::from(vec![animals_field, n_legs_field])),
    ///         false,
    ///     )
    /// ]);
    ///
    /// let normalized = RecordBatch::try_new(Arc::new(schema), vec![a])
    ///     .expect("valid conversion")
    ///     .normalize(".", None)
    ///     .expect("valid normalization");
    ///
    /// let expected = RecordBatch::try_from_iter_with_nullable(vec![
    ///     ("a.animals", animals.clone(), true),
    ///     ("a.n_legs", n_legs.clone(), true),
    /// ])
    /// .expect("valid conversion");
    ///
    /// assert_eq!(expected, normalized);
    /// ```
    pub fn normalize(&self, separator: &str, max_level: Option<usize>) -> Result<Self, ArrowError> {
        let max_level = match max_level.unwrap_or(usize::MAX) {
            0 => usize::MAX,
            val => val,
        };
        let mut stack: Vec<(usize, &ArrayRef, Vec<&str>, &FieldRef)> = self
            .columns
            .iter()
            .zip(self.schema.fields())
            .rev()
            .map(|(c, f)| {
                let name_vec: Vec<&str> = vec![f.name()];
                (0, c, name_vec, f)
            })
            .collect();
        let mut columns: Vec<ArrayRef> = Vec::new();
        let mut fields: Vec<FieldRef> = Vec::new();

        while let Some((depth, c, name, field_ref)) = stack.pop() {
            match field_ref.data_type() {
                DataType::Struct(ff) if depth < max_level => {
                    // Need to zip these in reverse to maintain original order
                    for (cff, fff) in c.as_struct().columns().iter().zip(ff.into_iter()).rev() {
                        let mut name = name.clone();
                        name.push(separator);
                        name.push(fff.name());
                        stack.push((depth + 1, cff, name, fff))
                    }
                }
                _ => {
                    let updated_field = Field::new(
                        name.concat(),
                        field_ref.data_type().clone(),
                        field_ref.is_nullable(),
                    );
                    columns.push(c.clone());
                    fields.push(Arc::new(updated_field));
                }
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }

    /// Returns the number of columns in the record batch.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    ///
    /// assert_eq!(batch.num_columns(), 1);
    /// ```
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of rows in each column.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    ///
    /// assert_eq!(batch.num_rows(), 5);
    /// ```
    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    /// Get a reference to a column's array by index.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside of `0..num_columns`.
    pub fn column(&self, index: usize) -> &ArrayRef {
        &self.columns[index]
    }

    /// Get a reference to a column's array by name.
    pub fn column_by_name(&self, name: &str) -> Option<&ArrayRef> {
        self.schema()
            .column_with_name(name)
            .map(|(index, _)| &self.columns[index])
    }

    /// Get a reference to all columns in the record batch.
    pub fn columns(&self) -> &[ArrayRef] {
        &self.columns[..]
    }

    /// Remove column by index and return it.
    ///
    /// Return the `ArrayRef` if the column is removed.
    ///
    /// # Panics
    ///
    /// Panics if `index`` out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow_array::{BooleanArray, Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Field, Schema};
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let bool_array = BooleanArray::from(vec![true, false, false, true, true]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    ///     Field::new("bool", DataType::Boolean, false),
    /// ]);
    ///
    /// let mut batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array), Arc::new(bool_array)]).unwrap();
    ///
    /// let removed_column = batch.remove_column(0);
    /// assert_eq!(removed_column.as_any().downcast_ref::<Int32Array>().unwrap(), &Int32Array::from(vec![1, 2, 3, 4, 5]));
    /// assert_eq!(batch.num_columns(), 1);
    /// ```
    pub fn remove_column(&mut self, index: usize) -> ArrayRef {
        let mut builder = SchemaBuilder::from(self.schema.as_ref());
        builder.remove(index);
        self.schema = Arc::new(builder.finish());
        self.columns.remove(index)
    }

    /// THIS METHOD IS COMMON, MODIFIED BY ARAS
    ///
    /// Return a new RecordBatch where each column is sliced
    /// according to `offset` and `length`
    ///
    /// # Panics
    ///
    /// Panics if `offset` with `length` is greater than column length.
    pub fn slice(&self, offset: usize, length: usize) -> RecordBatch {
        assert!((offset + length) <= self.num_rows());

        let columns = self
            .columns()
            .iter()
            .map(|column| column.slice(offset, length))
            .collect();

        Self {
            schema: self.schema.clone(),
            columns,
            row_count: length,
            metadata_flags: self.metadata_flags,
        }
    }

    /// Create a `RecordBatch` from an iterable list of pairs of the
    /// form `(field_name, array)`, with the same requirements on
    /// fields and arrays as [`RecordBatch::try_new`]. This method is
    /// often used to create a single `RecordBatch` from arrays,
    /// e.g. for testing.
    ///
    /// The resulting schema is marked as nullable for each column if
    /// the array for that column is has any nulls. To explicitly
    /// specify nullibility, use [`RecordBatch::try_from_iter_with_nullable`]
    ///
    /// Example:
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    ///
    /// let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    /// let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    ///
    /// let record_batch = RecordBatch::try_from_iter(vec![
    ///   ("a", a),
    ///   ("b", b),
    /// ]);
    /// ```
    /// Another way to quickly create a [`RecordBatch`] is to use the [`record_batch!`] macro,
    /// which is particularly helpful for rapid prototyping and testing.
    ///
    /// Example:
    ///
    /// ```rust
    /// use arrow_array::record_batch;
    /// let batch = record_batch!(
    ///     ("a", Int32, [1, 2, 3]),
    ///     ("b", Float64, [Some(4.0), None, Some(5.0)]),
    ///     ("c", Utf8, ["alpha", "beta", "gamma"])
    /// );
    /// ```
    pub fn try_from_iter<I, F>(value: I) -> Result<Self, ArrowError>
    where
        I: IntoIterator<Item = (F, ArrayRef)>,
        F: AsRef<str>,
    {
        // TODO: implement `TryFrom` trait, once
        // https://github.com/rust-lang/rust/issues/50133 is no longer an
        // issue
        let iter = value.into_iter().map(|(field_name, array)| {
            let nullable = array.null_count() > 0;
            (field_name, array, nullable)
        });

        Self::try_from_iter_with_nullable(iter)
    }

    /// Create a `RecordBatch` from an iterable list of tuples of the
    /// form `(field_name, array, nullable)`, with the same requirements on
    /// fields and arrays as [`RecordBatch::try_new`]. This method is often
    /// used to create a single `RecordBatch` from arrays, e.g. for
    /// testing.
    ///
    /// Example:
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    ///
    /// let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    /// let b: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
    ///
    /// // Note neither `a` nor `b` has any actual nulls, but we mark
    /// // b an nullable
    /// let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
    ///   ("a", a, false),
    ///   ("b", b, true),
    /// ]);
    /// ```
    pub fn try_from_iter_with_nullable<I, F>(value: I) -> Result<Self, ArrowError>
    where
        I: IntoIterator<Item = (F, ArrayRef, bool)>,
        F: AsRef<str>,
    {
        let iter = value.into_iter();
        let capacity = iter.size_hint().0;
        let mut schema = SchemaBuilder::with_capacity(capacity);
        let mut columns = Vec::with_capacity(capacity);

        for (field_name, array, nullable) in iter {
            let field_name = field_name.as_ref();
            schema.push(Field::new(field_name, array.data_type().clone(), nullable));
            columns.push(array);
        }

        let schema = Arc::new(schema.finish());
        RecordBatch::try_new(schema, columns)
    }

    /// Returns the total number of bytes of memory occupied physically by this batch.
    ///
    /// Note that this does not always correspond to the exact memory usage of a
    /// `RecordBatch` (might overestimate), since multiple columns can share the same
    /// buffers or slices thereof, the memory used by the shared buffers might be
    /// counted multiple times.
    pub fn get_array_memory_size(&self) -> usize {
        self.columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum()
    }

    /// THIS METHOD IS ARAS ONLY
    ///
    /// Gets the metadata_flags of RecordBatch
    pub fn metadata_flags(&self) -> u8 {
        self.metadata_flags
    }

    /// THIS METHOD IS ARAS ONLY
    ///
    /// Sets the metadata_flags of RecordBatch and returns self
    pub fn with_metadata_flags(mut self, metadata_flags: u8) -> Self {
        self.metadata_flags = metadata_flags;
        self
    }
}

/// THIS STRUCT IS COMMON, MODIFIED BY ARAS
///
/// Options that control the behaviour used when creating a [`RecordBatch`].
#[derive(Debug)]
#[non_exhaustive]
pub struct RecordBatchOptions {
    /// Match field names of structs and lists. If set to `true`, the names must match.
    pub match_field_names: bool,

    /// Optional row count, useful for specifying a row count for a RecordBatch with no columns
    pub row_count: Option<usize>,

    /// THIS MEMBER IS ARAS ONLY
    ///
    /// This tag is used to store metadata flags for the record batch.
    pub metadata_flags: u8,
}

impl RecordBatchOptions {
    /// Creates a new `RecordBatchOptions`
    pub fn new() -> Self {
        Self {
            match_field_names: true,
            row_count: None,
            metadata_flags: 0,
        }
    }
    /// Sets the row_count of RecordBatchOptions and returns self
    pub fn with_row_count(mut self, row_count: Option<usize>) -> Self {
        self.row_count = row_count;
        self
    }
    /// Sets the match_field_names of RecordBatchOptions and returns self
    pub fn with_match_field_names(mut self, match_field_names: bool) -> Self {
        self.match_field_names = match_field_names;
        self
    }
    /// THIS METHOD IS ARAS ONLY
    ///
    /// Sets the metadata_flags of RecordBatchOptions and returns self
    pub fn with_metadata_flags(mut self, metadata_flags: u8) -> Self {
        self.metadata_flags = metadata_flags;
        self
    }
}
impl Default for RecordBatchOptions {
    fn default() -> Self {
        Self::new()
    }
}
impl From<StructArray> for RecordBatch {
    /// THIS METHOD IS COMMON, MODIFIED BY ARAS
    fn from(value: StructArray) -> Self {
        let row_count = value.len();
        let (fields, columns, nulls) = value.into_parts();
        assert_eq!(
            nulls.map(|n| n.null_count()).unwrap_or_default(),
            0,
            "Cannot convert nullable StructArray to RecordBatch, see StructArray documentation"
        );

        RecordBatch {
            schema: Arc::new(Schema::new(fields)),
            row_count,
            columns,
            metadata_flags: 0,
        }
    }
}

impl From<&StructArray> for RecordBatch {
    fn from(struct_array: &StructArray) -> Self {
        struct_array.clone().into()
    }
}

impl Index<&str> for RecordBatch {
    type Output = ArrayRef;

    /// Get a reference to a column's array by name.
    ///
    /// # Panics
    ///
    /// Panics if the name is not in the schema.
    fn index(&self, name: &str) -> &Self::Output {
        self.column_by_name(name).unwrap()
    }
}

/// Generic implementation of [RecordBatchReader] that wraps an iterator.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, RecordBatchIterator, RecordBatchReader};
/// #
/// let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
/// let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
///
/// let record_batch = RecordBatch::try_from_iter(vec![
///   ("a", a),
///   ("b", b),
/// ]).unwrap();
///
/// let batches: Vec<RecordBatch> = vec![record_batch.clone(), record_batch.clone()];
///
/// let mut reader = RecordBatchIterator::new(batches.into_iter().map(Ok), record_batch.schema());
///
/// assert_eq!(reader.schema(), record_batch.schema());
/// assert_eq!(reader.next().unwrap().unwrap(), record_batch);
/// # assert_eq!(reader.next().unwrap().unwrap(), record_batch);
/// # assert!(reader.next().is_none());
/// ```
pub struct RecordBatchIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, ArrowError>>,
{
    inner: I::IntoIter,
    inner_schema: SchemaRef,
}

impl<I> RecordBatchIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, ArrowError>>,
{
    /// Create a new [RecordBatchIterator].
    ///
    /// If `iter` is an infallible iterator, use `.map(Ok)`.
    pub fn new(iter: I, schema: SchemaRef) -> Self {
        Self {
            inner: iter.into_iter(),
            inner_schema: schema,
        }
    }
}

impl<I> Iterator for RecordBatchIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, ArrowError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I> RecordBatchReader for RecordBatchIterator<I>
where
    I: IntoIterator<Item = Result<RecordBatch, ArrowError>>,
{
    fn schema(&self) -> SchemaRef {
        self.inner_schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BooleanArray, Int32Array, Int64Array, Int8Array, ListArray, StringArray, StringViewArray,
    };
    use arrow_buffer::{Buffer, ToByteSlice};
    use arrow_data::{ArrayData, ArrayDataBuilder};
    use arrow_schema::Fields;
    use std::collections::HashMap;

    #[test]
    fn create_record_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = StringArray::from(vec!["a", "b", "c", "d", "e"]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();
        check_batch(record_batch, 5)
    }

    #[test]
    fn create_string_view_record_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8View, false),
        ]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = StringViewArray::from(vec!["a", "b", "c", "d", "e"]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

        assert_eq!(5, record_batch.num_rows());
        assert_eq!(2, record_batch.num_columns());
        assert_eq!(&DataType::Int32, record_batch.schema().field(0).data_type());
        assert_eq!(
            &DataType::Utf8View,
            record_batch.schema().field(1).data_type()
        );
        assert_eq!(5, record_batch.column(0).len());
        assert_eq!(5, record_batch.column(1).len());
    }

    #[test]
    fn byte_size_should_not_regress() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = StringArray::from(vec!["a", "b", "c", "d", "e"]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();
        assert_eq!(record_batch.get_array_memory_size(), 364);
    }

    fn check_batch(record_batch: RecordBatch, num_rows: usize) {
        assert_eq!(num_rows, record_batch.num_rows());
        assert_eq!(2, record_batch.num_columns());
        assert_eq!(&DataType::Int32, record_batch.schema().field(0).data_type());
        assert_eq!(&DataType::Utf8, record_batch.schema().field(1).data_type());
        assert_eq!(num_rows, record_batch.column(0).len());
        assert_eq!(num_rows, record_batch.column(1).len());
    }

    #[test]
    #[should_panic(expected = "assertion failed: (offset + length) <= self.num_rows()")]
    fn create_record_batch_slice() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let expected_schema = schema.clone();

        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let b = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "h", "i"]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

        let offset = 2;
        let length = 5;
        let record_batch_slice = record_batch.slice(offset, length);

        assert_eq!(record_batch_slice.schema().as_ref(), &expected_schema);
        check_batch(record_batch_slice, 5);

        let offset = 2;
        let length = 0;
        let record_batch_slice = record_batch.slice(offset, length);

        assert_eq!(record_batch_slice.schema().as_ref(), &expected_schema);
        check_batch(record_batch_slice, 0);

        let offset = 2;
        let length = 10;
        let _record_batch_slice = record_batch.slice(offset, length);
    }

    #[test]
    #[should_panic(expected = "assertion failed: (offset + length) <= self.num_rows()")]
    fn create_record_batch_slice_empty_batch() {
        let schema = Schema::empty();

        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let offset = 0;
        let length = 0;
        let record_batch_slice = record_batch.slice(offset, length);
        assert_eq!(0, record_batch_slice.schema().fields().len());

        let offset = 1;
        let length = 2;
        let _record_batch_slice = record_batch.slice(offset, length);
    }

    #[test]
    fn create_record_batch_try_from_iter() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));

        let record_batch =
            RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).expect("valid conversion");

        let expected_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, false),
        ]);
        assert_eq!(record_batch.schema().as_ref(), &expected_schema);
        check_batch(record_batch, 5);
    }

    #[test]
    fn create_record_batch_try_from_iter_with_nullable() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));

        // Note there are no nulls in a or b, but we specify that b is nullable
        let record_batch =
            RecordBatch::try_from_iter_with_nullable(vec![("a", a, false), ("b", b, true)])
                .expect("valid conversion");

        let expected_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        assert_eq!(record_batch.schema().as_ref(), &expected_schema);
        check_batch(record_batch, 5);
    }

    #[test]
    fn create_record_batch_schema_mismatch() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int64Array::from(vec![1, 2, 3, 4, 5]);

        let err = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: column types must match schema types, expected Int32 but found Int64 at column index 0");
    }

    // THIS TEST IS COMMON, MODIFIED BY ARAS
    #[test]
    fn create_record_batch_field_name_mismatch() {
        let fields = vec![
            Field::new("a1", DataType::Int32, false),
            Field::new_list("a2", Field::new_list_field(DataType::Int8, false), false),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new_struct("a", fields, true)]));

        let a1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let a2_child = Int8Array::from(vec![1, 2, 3, 4]);
        let a2 = ArrayDataBuilder::new(DataType::List(Arc::new(Field::new(
            "array",
            DataType::Int8,
            false,
        ))))
        .add_child_data(a2_child.into_data())
        .len(2)
        .add_buffer(Buffer::from([0i32, 3, 4].to_byte_slice()))
        .build()
        .unwrap();
        let a2: ArrayRef = Arc::new(ListArray::from(a2));
        let a = ArrayDataBuilder::new(DataType::Struct(Fields::from(vec![
            Field::new("aa1", DataType::Int32, false),
            Field::new("a2", a2.data_type().clone(), false),
        ])))
        .add_child_data(a1.into_data())
        .add_child_data(a2.into_data())
        .len(2)
        .build()
        .unwrap();
        let a: ArrayRef = Arc::new(StructArray::from(a));

        // creating the batch with field name validation should fail
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()]);
        assert!(batch.is_err());

        // creating the batch without field name validation should pass
        let options = RecordBatchOptions {
            match_field_names: false,
            row_count: None,
            metadata_flags: 0,
        };
        let batch = RecordBatch::try_new_with_options(schema, vec![a], &options);
        assert!(batch.is_ok());
    }

    #[test]
    fn create_record_batch_record_mismatch() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]);
        assert!(batch.is_err());
    }

    #[test]
    fn create_record_batch_from_struct_array() {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
                int.clone() as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::from(&struct_array);
        assert_eq!(2, batch.num_columns());
        assert_eq!(4, batch.num_rows());
        assert_eq!(
            struct_array.data_type(),
            &DataType::Struct(batch.schema().fields().clone())
        );
        assert_eq!(batch.column(0).as_ref(), boolean.as_ref());
        assert_eq!(batch.column(1).as_ref(), int.as_ref());
    }

    #[test]
    fn record_batch_equality() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_eq!(batch1, batch2);
    }

    /// validates if the record batch can be accessed using `column_name` as index i.e. `record_batch["column_name"]`
    #[test]
    fn record_batch_index_access() {
        let id_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let val_arr = Arc::new(Int32Array::from(vec![5, 6, 7, 8]));
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);
        let record_batch =
            RecordBatch::try_new(Arc::new(schema1), vec![id_arr.clone(), val_arr.clone()]).unwrap();

        assert_eq!(record_batch["id"].as_ref(), id_arr.as_ref());
        assert_eq!(record_batch["val"].as_ref(), val_arr.as_ref());
    }

    #[test]
    fn record_batch_vals_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn record_batch_column_names_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("num", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn record_batch_column_number_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let num_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
            Field::new("num", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2), Arc::new(num_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn record_batch_row_count_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("num", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn normalize_simple() {
        let animals: ArrayRef = Arc::new(StringArray::from(vec!["Parrot", ""]));
        let n_legs: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(4)]));
        let year: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(2022)]));

        let animals_field = Arc::new(Field::new("animals", DataType::Utf8, true));
        let n_legs_field = Arc::new(Field::new("n_legs", DataType::Int64, true));
        let year_field = Arc::new(Field::new("year", DataType::Int64, true));

        let a = Arc::new(StructArray::from(vec![
            (animals_field.clone(), Arc::new(animals.clone()) as ArrayRef),
            (n_legs_field.clone(), Arc::new(n_legs.clone()) as ArrayRef),
            (year_field.clone(), Arc::new(year.clone()) as ArrayRef),
        ]));

        let month = Arc::new(Int64Array::from(vec![Some(4), Some(6)]));

        let schema = Schema::new(vec![
            Field::new(
                "a",
                DataType::Struct(Fields::from(vec![animals_field, n_legs_field, year_field])),
                false,
            ),
            Field::new("month", DataType::Int64, true),
        ]);

        let normalized =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![a.clone(), month.clone()])
                .expect("valid conversion")
                .normalize(".", Some(0))
                .expect("valid normalization");

        let expected = RecordBatch::try_from_iter_with_nullable(vec![
            ("a.animals", animals.clone(), true),
            ("a.n_legs", n_legs.clone(), true),
            ("a.year", year.clone(), true),
            ("month", month.clone(), true),
        ])
        .expect("valid conversion");

        assert_eq!(expected, normalized);

        // check 0 and None have the same effect
        let normalized = RecordBatch::try_new(Arc::new(schema), vec![a, month.clone()])
            .expect("valid conversion")
            .normalize(".", None)
            .expect("valid normalization");

        assert_eq!(expected, normalized);
    }

    #[test]
    fn normalize_nested() {
        // Initialize schema
        let a = Arc::new(Field::new("a", DataType::Int64, true));
        let b = Arc::new(Field::new("b", DataType::Int64, false));
        let c = Arc::new(Field::new("c", DataType::Int64, true));

        let one = Arc::new(Field::new(
            "1",
            DataType::Struct(Fields::from(vec![a.clone(), b.clone(), c.clone()])),
            false,
        ));
        let two = Arc::new(Field::new(
            "2",
            DataType::Struct(Fields::from(vec![a.clone(), b.clone(), c.clone()])),
            true,
        ));

        let exclamation = Arc::new(Field::new(
            "!",
            DataType::Struct(Fields::from(vec![one.clone(), two.clone()])),
            false,
        ));

        let schema = Schema::new(vec![exclamation.clone()]);

        // Initialize fields
        let a_field = Int64Array::from(vec![Some(0), Some(1)]);
        let b_field = Int64Array::from(vec![Some(2), Some(3)]);
        let c_field = Int64Array::from(vec![None, Some(4)]);

        let one_field = StructArray::from(vec![
            (a.clone(), Arc::new(a_field.clone()) as ArrayRef),
            (b.clone(), Arc::new(b_field.clone()) as ArrayRef),
            (c.clone(), Arc::new(c_field.clone()) as ArrayRef),
        ]);
        let two_field = StructArray::from(vec![
            (a.clone(), Arc::new(a_field.clone()) as ArrayRef),
            (b.clone(), Arc::new(b_field.clone()) as ArrayRef),
            (c.clone(), Arc::new(c_field.clone()) as ArrayRef),
        ]);

        let exclamation_field = Arc::new(StructArray::from(vec![
            (one.clone(), Arc::new(one_field) as ArrayRef),
            (two.clone(), Arc::new(two_field) as ArrayRef),
        ]));

        // Normalize top level
        let normalized =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![exclamation_field.clone()])
                .expect("valid conversion")
                .normalize(".", Some(1))
                .expect("valid normalization");

        let expected = RecordBatch::try_from_iter_with_nullable(vec![
            (
                "!.1",
                Arc::new(StructArray::from(vec![
                    (a.clone(), Arc::new(a_field.clone()) as ArrayRef),
                    (b.clone(), Arc::new(b_field.clone()) as ArrayRef),
                    (c.clone(), Arc::new(c_field.clone()) as ArrayRef),
                ])) as ArrayRef,
                false,
            ),
            (
                "!.2",
                Arc::new(StructArray::from(vec![
                    (a.clone(), Arc::new(a_field.clone()) as ArrayRef),
                    (b.clone(), Arc::new(b_field.clone()) as ArrayRef),
                    (c.clone(), Arc::new(c_field.clone()) as ArrayRef),
                ])) as ArrayRef,
                true,
            ),
        ])
        .expect("valid conversion");

        assert_eq!(expected, normalized);

        // Normalize all levels
        let normalized = RecordBatch::try_new(Arc::new(schema), vec![exclamation_field])
            .expect("valid conversion")
            .normalize(".", None)
            .expect("valid normalization");

        let expected = RecordBatch::try_from_iter_with_nullable(vec![
            ("!.1.a", Arc::new(a_field.clone()) as ArrayRef, true),
            ("!.1.b", Arc::new(b_field.clone()) as ArrayRef, false),
            ("!.1.c", Arc::new(c_field.clone()) as ArrayRef, true),
            ("!.2.a", Arc::new(a_field.clone()) as ArrayRef, true),
            ("!.2.b", Arc::new(b_field.clone()) as ArrayRef, false),
            ("!.2.c", Arc::new(c_field.clone()) as ArrayRef, true),
        ])
        .expect("valid conversion");

        assert_eq!(expected, normalized);
    }

    #[test]
    fn normalize_empty() {
        let animals_field = Arc::new(Field::new("animals", DataType::Utf8, true));
        let n_legs_field = Arc::new(Field::new("n_legs", DataType::Int64, true));
        let year_field = Arc::new(Field::new("year", DataType::Int64, true));

        let schema = Schema::new(vec![
            Field::new(
                "a",
                DataType::Struct(Fields::from(vec![animals_field, n_legs_field, year_field])),
                false,
            ),
            Field::new("month", DataType::Int64, true),
        ]);

        let normalized = RecordBatch::new_empty(Arc::new(schema.clone()))
            .normalize(".", Some(0))
            .expect("valid normalization");

        let expected = RecordBatch::new_empty(Arc::new(
            schema.normalize(".", Some(0)).expect("valid normalization"),
        ));

        assert_eq!(expected, normalized);
    }

    #[test]
    fn project() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let c: ArrayRef = Arc::new(StringArray::from(vec!["d", "e", "f"]));

        let record_batch =
            RecordBatch::try_from_iter(vec![("a", a.clone()), ("b", b.clone()), ("c", c.clone())])
                .expect("valid conversion");

        let expected =
            RecordBatch::try_from_iter(vec![("a", a), ("c", c)]).expect("valid conversion");

        assert_eq!(expected, record_batch.project(&[0, 2]).unwrap());
    }

    // THIS TEST IS COMMON, MODIFIED BY ARAS
    #[test]
    fn project_empty() {
        let c: ArrayRef = Arc::new(StringArray::from(vec!["d", "e", "f"]));

        let record_batch =
            RecordBatch::try_from_iter(vec![("c", c.clone())]).expect("valid conversion");

        let expected = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions {
                match_field_names: true,
                row_count: Some(3),
                metadata_flags: 0,
            },
        )
        .expect("valid conversion");

        assert_eq!(expected, record_batch.project(&[]).unwrap());
    }

    #[test]
    fn test_no_column_record_batch() {
        let schema = Arc::new(Schema::empty());

        let err = RecordBatch::try_new(schema.clone(), vec![]).unwrap_err();
        assert!(err
            .to_string()
            .contains("must either specify a row count or at least one column"));

        let options = RecordBatchOptions::new().with_row_count(Some(10));

        let ok = RecordBatch::try_new_with_options(schema.clone(), vec![], &options).unwrap();
        assert_eq!(ok.num_rows(), 10);

        let a = ok.slice(2, 5);
        assert_eq!(a.num_rows(), 5);

        let b = ok.slice(5, 0);
        assert_eq!(b.num_rows(), 0);

        assert_ne!(a, b);
        assert_eq!(b, RecordBatch::new_empty(schema))
    }

    #[test]
    fn test_nulls_in_non_nullable_field() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let maybe_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        );
        assert_eq!("Invalid argument error: Column 'a' is declared as non-nullable but contains null values", format!("{}", maybe_batch.err().unwrap()));
    }
    #[test]
    fn test_record_batch_options() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(false)
            .with_row_count(Some(20));
        assert!(!options.match_field_names);
        assert_eq!(options.row_count.unwrap(), 20)
    }

    #[test]
    #[should_panic(expected = "Cannot convert nullable StructArray to RecordBatch")]
    fn test_from_struct() {
        let s = StructArray::from(ArrayData::new_null(
            // Note child is not nullable
            &DataType::Struct(vec![Field::new("foo", DataType::Int32, false)].into()),
            2,
        ));
        let _ = RecordBatch::from(s);
    }

    #[test]
    fn test_with_schema() {
        let required_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let required_schema = Arc::new(required_schema);
        let nullable_schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let nullable_schema = Arc::new(nullable_schema);

        let batch = RecordBatch::try_new(
            required_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as _],
        )
        .unwrap();

        // Can add nullability
        let batch = batch.with_schema(nullable_schema.clone()).unwrap();

        // Cannot remove nullability
        batch.clone().with_schema(required_schema).unwrap_err();

        // Can add metadata
        let metadata = vec![("foo".to_string(), "bar".to_string())]
            .into_iter()
            .collect();
        let metadata_schema = nullable_schema.as_ref().clone().with_metadata(metadata);
        let batch = batch.with_schema(Arc::new(metadata_schema)).unwrap();

        // Cannot remove metadata
        batch.with_schema(nullable_schema).unwrap_err();
    }

    #[test]
    fn test_boxed_reader() {
        // Make sure we can pass a boxed reader to a function generic over
        // RecordBatchReader.
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let schema = Arc::new(schema);

        let reader = RecordBatchIterator::new(std::iter::empty(), schema);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);

        fn get_size(reader: impl RecordBatchReader) -> usize {
            reader.size_hint().0
        }

        let size = get_size(reader);
        assert_eq!(size, 0);
    }

    #[test]
    fn test_remove_column_maintains_schema_metadata() {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let bool_array = BooleanArray::from(vec![true, false, false, true, true]);

        let mut metadata = HashMap::new();
        metadata.insert("foo".to_string(), "bar".to_string());
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("bool", DataType::Boolean, false),
        ])
        .with_metadata(metadata);

        let mut batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(bool_array)],
        )
        .unwrap();

        let _removed_column = batch.remove_column(0);
        assert_eq!(batch.schema().metadata().len(), 1);
        assert_eq!(
            batch.schema().metadata().get("foo").unwrap().as_str(),
            "bar"
        );
    }
}
