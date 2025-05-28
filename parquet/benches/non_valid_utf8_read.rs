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

extern crate arrow;
extern crate criterion;

use arrow_data::UnsafeFlag;
use criterion::*;

use arrow::array::*;
// use arrow::csv;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ColumnValueDecoderOptions;
use parquet::arrow::DefaultValueForInvalidUtf8;
use std::sync::Arc;

fn generate_invalid_file() -> (std::fs::File, Vec<Option<Vec<u8>>>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        arrow_schema::DataType::Binary,
        true,
    )]));

    let mut raw = vec![Some(b"ok".to_vec()); 100];
    raw[1] = Some(vec![0xff, 0xfe]); // Invalid UTF-8

    // let raw = [
    //     Some(b"ok_1".to_vec()),
    //     Some(vec![0xff, 0xfe]), // Invalid UTF-8
    //     Some(b"ok_3".to_vec()),
    // ];
    let binary_array = Arc::new(BinaryArray::from(
        raw.iter().map(|x| x.as_deref()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![binary_array]).unwrap();
    let mut file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    (file, raw)
}

fn generate_valid_file() -> std::fs::File {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        arrow_schema::DataType::Binary,
        true,
    )]));
    let raw = [
        Some(b"ok_1".to_vec()),
        Some(b"ok_2".to_vec()), // Invalid UTF-8
        Some(b"ok_3".to_vec()),
    ];
    let binary_array = Arc::new(BinaryArray::from(
        raw.iter().map(|x| x.as_deref()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![binary_array]).unwrap();
    let mut file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    file
}

fn criterion_benchmark(c: &mut Criterion) {
    let (file, raw) = generate_invalid_file();

    // let test_cases = vec![
    //     (
    //         DefaultValueForInvalidUtf8::Default("__invalid__".to_string()),
    //         vec![Some("ok_1"), Some("__invalid__"), Some("ok_3")],
    //     ),
    //     (
    //         DefaultValueForInvalidUtf8::Null,
    //         vec![Some("ok_1"), None, Some("ok_3")],
    //     ),
    // ];

    c.bench_function("invalid_with_null", |b| {
        let mut expected = vec![];
        for (i, v) in raw.iter().enumerate() {
            if i == 1 {
                expected.push(None);
                continue;
            }

            if let Some(x) = v {
                expected.push(Some(std::str::from_utf8(x).unwrap()));
            } else {
                expected.push(None);
            }
        }

        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Null,
                false,
                expected.clone(),
            );
        });
    });

    c.bench_function("invalid_with_default_value", |b| {
        let mut expected = vec![];
        for (i, v) in raw.iter().enumerate() {
            if i == 1 {
                expected.push(None);
                continue;
            }

            if let Some(x) = v {
                expected.push(Some(std::str::from_utf8(x).unwrap()));
            } else {
                expected.push(None);
            }
        }

        expected[1] = Some("invalid");

        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Default("invalid".to_string()),
                false,
                expected.clone(),
            );
        });
    });

    let file = generate_valid_file();

    c.bench_function("valid_with_null", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Null,
                false,
                vec![Some("ok_1"), Some("ok_2"), Some("ok_3")],
            );
        });
    });

    c.bench_function("valid_with_default_value", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::Default("invalid".to_string()),
                false,
                vec![Some("ok_1"), Some("ok_2"), Some("ok_3")],
            );
        });
    });

    c.bench_function("valid_with_none", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::None,
                false,
                vec![Some("ok_1"), Some("ok_2"), Some("ok_3")],
            );
        });
    });

    c.bench_function("valid_with_skip", |b| {
        b.iter(|| {
            validate_utf8_decoding(
                &file,
                DefaultValueForInvalidUtf8::None,
                true,
                vec![Some("ok_1"), Some("ok_2"), Some("ok_3")],
            );
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn validate_utf8_decoding(
    file: &std::fs::File,
    default_value: DefaultValueForInvalidUtf8,
    to_skip: bool,
    expected: Vec<Option<&str>>,
) {
    let projected_schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        arrow_schema::DataType::Utf8,
        true,
    )]));

    let mut flag = UnsafeFlag::new();
    if to_skip {
        unsafe {
            flag.set(true);
        }
    }
    let opts = ArrowReaderOptions::new()
        .with_schema(projected_schema.clone())
        .with_column_value_decoder_options(ColumnValueDecoderOptions::new(
            flag.clone(),
            default_value.clone(),
        ));

    let metadata = ArrowReaderMetadata::load(file, opts.clone()).unwrap();
    let builder =
        ParquetRecordBatchReaderBuilder::new_with_metadata(file.try_clone().unwrap(), metadata)
            .with_column_value_decoder_options(opts.column_value_decoder_options);
    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.len(), expected.len());

    for (i, expected_val) in expected.iter().enumerate() {
        match expected_val {
            Some(expected_str) => assert_eq!(arr.value(i), *expected_str),
            None => assert!(arr.is_null(i), "Expected null at index {}", i),
        }
    }
}
