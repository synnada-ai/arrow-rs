// Copyright (C) Synnada, Inc. - All Rights Reserved.
// This file does not contain any Apache Software Foundation (ASF) licensed code.

// ============================= BENCHMARK RESULTS =============================
// Latest run with N_ROWS = 100,000 and ~1% invalid UTF-8 entries
//
// | Benchmark                  | Time (ms)          | vs Baseline |
// |----------------------------|--------------------|-------------|
// | valid_binary_baseline      | 17.810 - 19.014    |    100.0%   |
// | valid_with_skip            | 14.848 - 15.203    |     82.5%   |
// | valid_with_none            | 26.651 - 27.052    |    146.9%   |
// | valid_with_null            | 26.588 - 27.133    |    148.0%   |
// | valid_with_default_value   | 27.291 - 27.826    |    151.2%   |
// | invalid_with_null          | 31.562 - 32.874    |    173.1%   |
// | invalid_with_default_value | 30.594 - 32.030    |    168.6%   |

extern crate arrow;
extern crate criterion;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow_data::UnsafeFlag;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::{ArrowWriter, ColumnValueDecoderOptions, DefaultValueForInvalidUtf8};
use parquet::file::properties::WriterProperties;

use criterion::*;

const N_ROWS: usize = 100_000;

fn generate_invalid_file() -> std::fs::File {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        DataType::Binary,
        true,
    )]));

    // Create longer strings to make UTF-8 validation more expensive
    let long_string =
        "Hello, World! This is a longer string to make UTF-8 validation take more time. 🎉"
            .repeat(10);
    let mut raw = vec![Some(long_string.as_bytes().to_vec()); N_ROWS];
    // Make approximately 1% of entries invalid UTF-8 (every 100th element)
    for i in (0..N_ROWS).step_by(100) {
        raw[i] = Some(vec![0xff, 0xfe]); // Invalid UTF-8
    }

    let binary_array = Arc::new(BinaryArray::from(
        raw.iter().map(|x| x.as_deref()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![binary_array]).unwrap();

    // Create writer properties with large row group size and disable dictionary encoding
    let props = WriterProperties::builder()
        .set_max_row_group_size(N_ROWS)
        .set_dictionary_enabled(false) // Disable dictionary encoding
        .build();

    let mut file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    file
}

fn generate_valid_file() -> std::fs::File {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "item",
        DataType::Binary,
        true,
    )]));
    // Same longer strings as invalid file for consistency
    let long_string =
        "Hello, World! This is a longer string to make UTF-8 validation take more time. 🎉"
            .repeat(10);
    let raw = vec![Some(long_string.as_bytes().to_vec()); N_ROWS];
    let binary_array = Arc::new(BinaryArray::from(
        raw.iter().map(|x| x.as_deref()).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![binary_array]).unwrap();

    // Create writer properties with large row group size and disable dictionary encoding
    let props = WriterProperties::builder()
        .set_max_row_group_size(N_ROWS)
        .set_dictionary_enabled(false) // Disable dictionary encoding
        .build();

    let mut file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    file
}

fn prepare_reader(
    file: &std::fs::File,
    default_value: DefaultValueForInvalidUtf8,
    to_skip: bool,
) -> ParquetRecordBatchReader {
    let projected_schema = Arc::new(Schema::new(vec![Field::new("item", DataType::Utf8, true)]));

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

    builder.build().unwrap()
}

fn prepare_binary_reader(file: &std::fs::File) -> ParquetRecordBatchReader {
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
    builder.build().unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let invalid_file = generate_invalid_file();
    let valid_file = generate_valid_file();

    c.bench_function("invalid_with_null", |b| {
        b.iter(|| {
            let reader = prepare_reader(&invalid_file, DefaultValueForInvalidUtf8::Null, false);
            // Just consume all batches without collecting
            for batch in reader {
                let _ = black_box(batch.unwrap());
            }
        });
    });

    c.bench_function("invalid_with_default_value", |b| {
        b.iter(|| {
            let reader = prepare_reader(
                &invalid_file,
                DefaultValueForInvalidUtf8::Default("invalid".to_string()),
                false,
            );
            // Just consume all batches without collecting
            for batch in reader {
                let _ = black_box(batch.unwrap());
            }
        });
    });

    // Baseline: pure binary read without UTF-8 conversion
    c.bench_function("valid_binary_baseline", |b| {
        b.iter(|| {
            let reader = prepare_binary_reader(&valid_file);
            // Just consume all batches
            for batch in reader {
                let _ = black_box(batch.unwrap());
            }
        });
    });

    c.bench_function("valid_with_null", |b| {
        b.iter(|| {
            let reader = prepare_reader(&valid_file, DefaultValueForInvalidUtf8::Null, false);
            // Just consume all batches
            for batch in reader {
                let _ = black_box(batch.unwrap());
            }
        });
    });

    c.bench_function("valid_with_default_value", |b| {
        b.iter(|| {
            let reader = prepare_reader(
                &valid_file,
                DefaultValueForInvalidUtf8::Default("invalid".to_string()),
                false,
            );
            // Just consume all batches
            for batch in reader {
                let _ = black_box(batch.unwrap());
            }
        });
    });

    c.bench_function("valid_with_none", |b| {
        b.iter(|| {
            let reader = prepare_reader(&valid_file, DefaultValueForInvalidUtf8::None, false);
            // Just consume all batches
            for batch in reader {
                let _ = black_box(batch.unwrap());
            }
        });
    });

    c.bench_function("valid_with_skip", |b| {
        b.iter(|| {
            let reader = prepare_reader(&valid_file, DefaultValueForInvalidUtf8::None, true);
            // Just consume all batches
            for batch in reader {
                let _ = black_box(batch.unwrap());
            }
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
