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

use crate::Reader;
use arrow_array::builder::{Decimal128Builder, Decimal256Builder};
use arrow_array::types::{Float64Type, Int32Type, UInt32Type};
use arrow_array::{
    BooleanArray, Date32Array, Date64Array, DictionaryArray, PrimitiveArray, RecordBatch,
    StringArray, Time32SecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_buffer::i256;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use std::io::Cursor;
use std::sync::Arc;

pub(crate) fn test_write_csv_case() -> Result<(Vec<RecordBatch>, String), ArrowError> {
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::UInt32, false),
        Field::new("c4", DataType::Boolean, true),
        Field::new("c5", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        Field::new(
            "c7",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]);

    let c1 = StringArray::from(vec![
        "Lorem ipsum dolor sit amet",
        "consectetur adipiscing elit",
        "sed do eiusmod tempor",
    ]);
    let c2 = PrimitiveArray::<Float64Type>::from(vec![
        Some(123.564532),
        None,
        Some(-556132.25),
    ]);
    let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
    let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
    let c5 = TimestampMillisecondArray::from(vec![
        None,
        Some(1555584887378),
        Some(1555555555555),
    ]);
    let c6 = Time32SecondArray::from(vec![1234, 24680, 85563]);
    let c7: DictionaryArray<Int32Type> =
        vec!["cupcakes", "cupcakes", "foo"].into_iter().collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(c1),
            Arc::new(c2),
            Arc::new(c3),
            Arc::new(c4),
            Arc::new(c5),
            Arc::new(c6),
            Arc::new(c7),
        ],
    )?;

    let expected = r#"c1,c2,c3,c4,c5,c6,c7
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34,cupcakes
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20,cupcakes
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03,foo
Lorem ipsum dolor sit amet,123.564532,3,true,,00:20:34,cupcakes
consectetur adipiscing elit,,2,false,2019-04-18T10:54:47.378000000,06:51:20,cupcakes
sed do eiusmod tempor,-556132.25,1,,2019-04-18T02:45:55.555000000,23:46:03,foo
"#
    .to_owned();

    Ok((vec![batch.clone(), batch.clone()], expected))
}

pub(crate) fn test_write_csv_decimal_case(
) -> Result<(Vec<RecordBatch>, String), ArrowError> {
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Decimal128(38, 6), true),
        Field::new("c2", DataType::Decimal256(76, 6), true),
    ]);

    let mut c1_builder =
        Decimal128Builder::new().with_data_type(DataType::Decimal128(38, 6));
    c1_builder.extend(vec![Some(-3335724), Some(2179404), None, Some(290472)]);
    let c1 = c1_builder.finish();

    let mut c2_builder =
        Decimal256Builder::new().with_data_type(DataType::Decimal256(76, 6));
    c2_builder.extend(vec![
        Some(i256::from_i128(-3335724)),
        Some(i256::from_i128(2179404)),
        None,
        Some(i256::from_i128(290472)),
    ]);
    let c2 = c2_builder.finish();

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)])?;
    let batches = vec![batch.clone(), batch];
    let expected = r#"c1,c2
-3.335724,-3.335724
2.179404,2.179404
,
0.290472,0.290472
-3.335724,-3.335724
2.179404,2.179404
,
0.290472,0.290472
"#
    .to_owned();
    Ok((batches, expected))
}

pub(crate) fn test_write_csv_custom_options_case(
) -> Result<(Vec<RecordBatch>, String), ArrowError> {
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::UInt32, false),
        Field::new("c4", DataType::Boolean, true),
        Field::new("c6", DataType::Time32(TimeUnit::Second), false),
    ]);

    let c1 = StringArray::from(vec![
        "Lorem ipsum dolor sit amet",
        "consectetur adipiscing elit",
        "sed do eiusmod tempor",
    ]);
    let c2 = PrimitiveArray::<Float64Type>::from(vec![
        Some(123.564532),
        None,
        Some(-556132.25),
    ]);
    let c3 = PrimitiveArray::<UInt32Type>::from(vec![3, 2, 1]);
    let c4 = BooleanArray::from(vec![Some(true), Some(false), None]);
    let c6 = Time32SecondArray::from(vec![1234, 24680, 85563]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(c1),
            Arc::new(c2),
            Arc::new(c3),
            Arc::new(c4),
            Arc::new(c6),
        ],
    )?;
    let batches = vec![batch];
    let expected = "Lorem ipsum dolor sit amet|123.564532|3|true|12:20:34 AM\nconsectetur adipiscing elit|NULL|2|false|06:51:20 AM\nsed do eiusmod tempor|-556132.25|1|NULL|11:46:03 PM\n".to_owned();
    Ok((batches, expected))
}

pub(crate) fn test_write_csv_using_rfc3339_case(
) -> Result<(Vec<RecordBatch>, String), ArrowError> {
    let schema = Schema::new(vec![
        Field::new(
            "c1",
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".to_string())),
            true,
        ),
        Field::new("c2", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("c3", DataType::Date32, false),
        Field::new("c4", DataType::Time32(TimeUnit::Second), false),
    ]);

    let c1 =
        TimestampMillisecondArray::from(vec![Some(1555584887378), Some(1635577147000)])
            .with_timezone("+00:00".to_string());
    let c2 =
        TimestampMillisecondArray::from(vec![Some(1555584887378), Some(1635577147000)]);
    let c3 = Date32Array::from(vec![3, 2]);
    let c4 = Time32SecondArray::from(vec![1234, 24680]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
    )?;
    let batches = vec![batch];
    let expected = "c1,c2,c3,c4
2019-04-18T10:54:47.378Z,2019-04-18T10:54:47.378,1970-01-04,00:20:34
2021-10-30T06:59:07Z,2021-10-30T06:59:07,1970-01-03,06:51:20\n"
        .to_owned();
    Ok((batches, expected))
}

pub(crate) fn test_conversion_consistency_case(
    buf: &mut Cursor<Vec<u8>>,
    schema: SchemaRef,
    nanoseconds: Vec<i64>,
) -> Result<(), ArrowError> {
    // test if we can serialize and deserialize whilst retaining the same type information/ precision
    let mut reader = Reader::new(
        buf, schema, false, None, 3, // starting at row 2 and up to row 6.
        None, None, None,
    );
    let rb = reader.next().unwrap().unwrap();
    let c1 = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    let c2 = rb.column(1).as_any().downcast_ref::<Date64Array>().unwrap();
    let c3 = rb
        .column(2)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    let actual = c1.into_iter().collect::<Vec<_>>();
    let expected = vec![Some(3), Some(2), Some(1)];
    assert_eq!(actual, expected);
    let actual = c2.into_iter().collect::<Vec<_>>();
    let expected = vec![Some(3), Some(2), Some(1)];
    assert_eq!(actual, expected);
    let actual = c3.into_iter().collect::<Vec<_>>();
    let expected = nanoseconds.into_iter().map(Some).collect::<Vec<_>>();
    assert_eq!(actual, expected);
    Ok(())
}
