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

//! Specialized decoders optimised for decoding to arrow format

mod delta_byte_array;
mod dictionary_index;

use arrow_data::UnsafeFlag;

pub use delta_byte_array::DeltaByteArrayDecoder;
pub use dictionary_index::DictIndexDecoder;

/// Options for column value decoding behavior.
///
/// Contains settings that control how column values are decoded, such as
/// whether to validate decoded values.
///
/// Setting `skip_validation` to true may improve performance but could
/// result in incorrect data if the input is malformed.
#[derive(Debug, Default, Clone)]
pub struct ColumnValueDecoderOptions {
    /// Skip validation of the values read from the column.
    pub skip_validation: UnsafeFlag,
}

impl ColumnValueDecoderOptions {
    /// Create a new `ColumnValueDecoderOptions` with the given `skip_validation` flag.
    pub fn new(skip_validation: UnsafeFlag) -> Self {
        Self { skip_validation }
    }
}
