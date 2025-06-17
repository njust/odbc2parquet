use std::path::PathBuf;

use bytesize::ByteSize;
use clap::{ArgAction, Args};
use io_arg::IoArg;
use parquet::basic::Encoding;

use crate::enum_args::{column_encoding_from_str, EncodingArgument};
use crate::{connection::ConnectOpts, enum_args::CompressionVariants};

#[derive(Args)]
pub struct QueryOpt {
    #[clap(flatten)]
    pub connect_opts: ConnectOpts,
    /// Size of a single batch in rows. The content of the data source is written into the output
    /// parquet files in batches. This way the content does never need to be materialized completely
    /// in memory at once. If `--batch-size-memory` is not specified this value defaults to 65535.
    /// This avoids issues with some ODBC drivers using 16Bit integers to represent batch sizes. If
    /// `--batch-size-memory` is specified no other limit is applied by default. If both option are
    /// specified the batch size is the largest possible which satisfies both constraints.
    #[arg(long)]
    pub batch_size_row: Option<usize>,
    /// Limits the size of a single batch. It does so by calculating the amount of memory each row
    /// requires in the allocated buffers and then limits the maximum number of rows so that the
    /// maximum buffer size comes as close as possible, but does not exceed the specified amount.
    /// Default is 2GiB on 64 Bit platforms and 1GiB on 32 Bit Platforms if `--batch-size-row` is
    /// not specified. If `--batch-size-row` is not specified no memory limit is applied by default.
    /// If both option are specified the batch size is the largest possible which satisfies both
    /// constraints. This option controls the size of the buffers of data in transit, and therefore
    /// the memory usage of this tool. It indirectly controls the size of the row groups written to
    /// parquet (since each batch is written as one row group). It is hard to make a generic
    /// statement about how much smaller the average row group will be.
    /// This options allows you to specify the memory usage using SI units. So you can pass `2Gib`,
    /// `600Mb` and so on.
    #[arg(long)]
    pub batch_size_memory: Option<ByteSize>,
    /// Maximum number of batches in a single output parquet file. If this option is omitted or 0 a
    /// single output file is produces. Otherwise each output file is closed after the maximum
    /// number of batches have been written and a new one with the suffix `_n` is started. There n
    /// is the of the produced output file starting at one for the first one. E.g. `out_01.par`,
    /// `out_2.par`, ...
    #[arg(long, default_value = "0")]
    pub row_groups_per_file: u32,
    /// Trade speed for memory. If `true`, only one fetch buffer is allocated. It usually takes way
    /// more memory than the buffers required to write into parquet, since it contains the data
    /// uncompressed and must be able to hold the largest possible value of fields, even if the
    /// actual data is small. So only using one instead of two usually halfes the required memory,
    /// yet it blocks fetching the next batch from the database, until the contents of the current
    /// one have been written. This can slow down the creation of parquet up to a factor of two in
    /// in case writing to parquet takes just as much time as fetching from the database. Usually
    /// io to the database is the bottlneck so the actual slow down is likely lower, but often still
    /// significant.
    #[arg(long)]
    pub sequential_fetching: bool,
    /// Then the size of the currently written parquet files goes beyond this threshold the current
    /// row group will be finished and then the file will be closed. So the file will be somewhat
    /// larger than the threshold. All further row groups will be written into new files to which
    /// the threshold size limit is applied as well. If this option is not set, no size threshold is
    /// applied. If the threshold is applied the first file name will have the suffix `_01`, the
    /// second the suffix `_2` and so on. Therefore, the first resulting file will be called e.g.
    /// `out_1.par`, if `out.par` has been specified as the output argument.
    /// Also note that this option will not act as an upper bound. It will act as a lower bound for
    /// all but the last file, all others however will not be larger than this threshold by more
    /// than the size of one row group. You can use the `batch_size_row` and `batch_size_memory`
    /// options to control the size of the row groups. Do not expect the `batch_size_memory` however
    /// to be equal to the row group size. The row group size depends on the actual data in the
    /// database, and is due to compression likely much smaller. Values of this option can be
    /// specified in SI units. E.g. `--file-size-threshold 1GiB`.
    #[arg(long)]
    pub file_size_threshold: Option<ByteSize>,
    /// You can use this to limit the transfer buffer size which is used for an individual variadic
    /// sized column.
    ///
    /// This is useful in situations there ODBC would require us to allocate a ridiculous amount of
    /// memory for a single element of a row. Usually this is the case because the Database schema
    /// has been ill-defined (like choosing `TEXT` for a username, although a users name is
    /// unlikely to be several GB long). Another situation is that the ODBC driver is not good at
    /// reporting the maximum length and therefore reports a really large value. The third option is
    /// of course that your values are actually large. In this case you just need a ton of memory.
    /// You can use the batch size limit though to retrieve less at once. For binary columns this is
    /// a maximum element length in bytes. For text columns it depends on whether UTF-8 or UTF-16
    /// encoding is used. See documentation of the `encoding` option. In case of UTF-8 this is the
    /// maximum length in bytes for an element. In case of UTF-16 the binary length is multiplied by
    /// two. This allows domain experts to configure limits (roughly) in the domain of how many
    /// letters do I expect in this column, rather than to care about whether the command is
    /// executed on Linux or Windows. The encoding of the column on the Database does not matter for
    /// this setting or determining buffer sizes.
    #[arg(long, default_value = "4096")]
    pub column_length_limit: usize,
    /// Default compression used by the parquet file writer.
    #[arg(long, value_enum, default_value = "zstd")]
    pub column_compression_default: CompressionVariants,
    /// The `gzip`, `zstd` and `brotli` compression variants allow for specifying an explicit
    /// compression level. If the selected compression variant does not support an explicit
    /// compression level this option is ignored.
    ///
    /// Default compression level for `zstd` is 3
    #[arg(long)]
    pub column_compression_level_default: Option<u32>,
    /// Encoding used for character data requested from the data source.
    ///
    /// `Utf16`: The tool will use 16Bit characters for requesting text from the data source,
    /// implying the use of UTF-16 encoding. This should work well independent of the system
    /// configuration, but implies additional work since text is always stored as UTF-8 in parquet.
    ///
    /// `System`: The tool will use 8Bit characters for requesting text from the data source,
    /// implying the use of the encoding from the system locale. This only works for non ASCII
    /// characters if the locales character set is UTF-8.
    ///
    /// `Auto`: Since on OS-X and Linux the default locales character set is always UTF-8 the
    /// default option is the same as `System` on non-windows platforms. On windows the default is
    /// `Utf16`.
    #[arg(long, value_enum, default_value = "Auto", ignore_case = true)]
    pub encoding: EncodingArgument,
    /// Map `BINARY` SQL columns to `BYTE_ARRAY` instead of `FIXED_LEN_BYTE_ARRAY`. This flag has
    /// been introduced in an effort to increase the compatibility of the output with Apache Spark.
    #[clap(long)]
    pub prefer_varbinary: bool,
    /// Specify the fallback encoding of the parquet output column. You can parse multiple values
    /// in format `COLUMN:ENCODING`. `ENCODING` must be one of: `plain`, `delta-binary-packed`,
    /// `delta-byte-array`, `delta-length-byte-array` or `rle`.
    #[arg(
        long,
        value_parser=column_encoding_from_str,
        action = ArgAction::Append
    )]
    pub parquet_column_encoding: Vec<(String, Encoding)>,
    /// Tells the odbc2parquet, that the ODBC driver does not support binding 64-Bit integers (aka
    /// S_C_BIGINT in ODBC speak). This will cause the odbc2parquet to query large integers as text
    /// instead and convert them to 64-Bit integers itself. Setting this flag will not affect the
    /// output, but may incur a performance penalty. In case you are using an Oracle Database it
    /// can make queries work which did not before, because Oracle does not support 64-Bit integers.
    #[clap(long)]
    pub driver_does_not_support_64bit_integers: bool,
    /// Use this flag if you want to avoid the logical type DECIMAL in the produced output. E.g.
    /// because you want to process it with polars which does not support DECIMAL. In case the scale
    /// of the relational Decimal type is 0, the output will be mapped to either 32Bit or 64Bit
    /// Integeres with logical type none. If the scale is not 0 the Decimal column will be fetches
    /// as text.
    #[clap(long)]
    pub avoid_decimal: bool,
    /// In case fetch results gets split into multiple files a suffix with a number will be appended
    /// to each file name. Default suffix length is 2 leading to suffixes like e.g. `_03`. In case
    /// you would expect thousands of files in your output you may want to set this to say `4` so
    /// the zeros pad this to a 4 digit number in order to make the filenames more friendly for
    /// lexical sorting.
    #[clap(long, default_value = "2")]
    pub suffix_length: usize,
    /// In case the query comes back with a result set, but now rows, by default a file with only
    /// schema information is still created. If you do not want to create any file in case the
    /// result set is empty you can set this flag.
    #[clap(long)]
    pub no_empty_file: bool,
    /// Name of the output parquet file. Use `-` to indicate that the output should be written to
    /// standard out instead. This option does nothing if the output is written to standard out.
    pub output: IoArg,
    /// Query executed against the ODBC data source. Question marks (`?`) can be used as
    /// placeholders for positional parameters. E.g. "SELECT Name FROM Employees WHERE salary > ?;".
    /// Instead of passing a query verbatim, you may pass a plain dash (`-`), to indicate that the
    /// query should be read from standard input. In this case the entire input until EOF will be
    /// considered the query.
    pub query: String,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    pub parameters: Vec<String>,
}

impl QueryOpt {
    pub fn from_connection_string(connection_string: &str, query: &str, output: PathBuf) -> Self {
        Self {
            connect_opts: ConnectOpts {
                prompt: false,
                connection_string: Some(connection_string.to_string()),
                dsn: None,
                user: None,
                password: None,
            },
            parameters: vec![],
            query: query.to_string(),
            batch_size_row: None,
            batch_size_memory: None,
            row_groups_per_file: 0,
            sequential_fetching: false,
            file_size_threshold: None,
            column_length_limit: 4096,
            column_compression_default: CompressionVariants::Zstd,
            column_compression_level_default: None,
            encoding: EncodingArgument::Auto,
            prefer_varbinary: false,
            parquet_column_encoding: vec![],
            driver_does_not_support_64bit_integers: false,
            avoid_decimal: false,
            suffix_length: 2,
            no_empty_file: false,
            output: IoArg::File(output),
        }
    }

    pub fn file_size_treshold(mut self, threshold: ByteSize) -> Self {
        self.file_size_threshold.replace(threshold);
        self
    }

    pub fn column_length_limit(mut self, limit: usize) -> Self {
        self.column_length_limit = limit;
        self
    }
}

#[derive(Args)]
pub struct InsertOpt {
    #[clap(flatten)]
    pub connect_opts: ConnectOpts,
    /// Encoding used for transferring character data to the database.
    ///
    /// `Utf16`: Use 16Bit characters to send text to the database, which implies the using
    /// UTF-16 encoding. This should work well independent of the system configuration, but requires
    /// additional work since text is always stored as UTF-8 in parquet.
    ///
    /// `System`: Use 8Bit characters for requesting text from the data source, implies using
    /// the encoding defined by the system locale. This only works for non ASCII characters if the
    /// locales character set is UTF-8.
    ///
    /// `Auto`: Since on OS-X and Linux the default locales character set is always UTF-8 the
    /// default option is the same as `System` on non-windows platforms. On windows the default is
    /// `Utf16`.
    #[arg(long, value_enum, default_value = "Auto", ignore_case = true)]
    pub encoding: EncodingArgument,
    /// Path to the input parquet file which is used to fill the database table with values.
    pub input: PathBuf,
    /// Name of the table to insert the values into. No precautions against SQL injection are
    /// taken. The insert statement is created by the tool. It will only work if the column names
    /// are the same in the parquet file and the database.
    pub table: String,
}

#[derive(Args)]
pub struct ExecOpt {
    #[clap(flatten)]
    pub connect_opts: ConnectOpts,
    /// Encoding used for transferring character data to the database.
    ///
    /// `Utf16`: Use 16Bit characters to send text to the database, which implies the using
    /// UTF-16 encoding. This should work well independent of the system configuration, but requires
    /// additional work since text is always stored as UTF-8 in parquet.
    ///
    /// `System`: Use 8Bit characters for requesting text from the data source, implies using
    /// the encoding defined by the system locale. This only works for non ASCII characters if the
    /// locales character set is UTF-8.
    ///
    /// `Auto`: Since on OS-X and Linux the default locales character set is always UTF-8 the
    /// default option is the same as `System` on non-windows platforms. On windows the default is
    /// `Utf16`.
    #[arg(long, value_enum, default_value = "Auto", ignore_case = true)]
    pub encoding: EncodingArgument,
    /// Path to the input parquet file which is used to fill the database table with values.
    pub input: PathBuf,
    /// SQL statement to execute. You can bind the columns of the parquet file to input parameters
    /// of the statement. You can do this by using the column name of the parquet file surrounded by
    /// question marks (`?`). E.g. `INSERT INTO table (col1, col2) VALUES (?col1?, ?col2?)`. In case
    /// you want to use the `?` in a capacity different from a placeholder it must be escaped with a
    /// backslash (`\?`). Backslashes must also be escaped with another backslash. Keep in mind that
    /// your shell may also need escaping for backslashes. You may need four backslashes in total to
    /// write a singe backslash in e.g. a string literal (`\\\\`).
    pub statement: String,
}
