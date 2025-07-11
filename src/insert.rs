use std::fs::File;

use anyhow::Error;
use log::info;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::{
    connection::open_connection, input::parquet_type_to_odbc_buffer_desc,
    parquet_buffer::ParquetBuffer, InsertOpt,
};

/// Read the content of a parquet file and insert it into a table.
pub fn insert(insert_opt: &InsertOpt) -> Result<(), Error> {
    let InsertOpt {
        encoding,
        input,
        connect_opts,
        table,
    } = insert_opt;

    let odbc_conn = open_connection(connect_opts)?;

    let file = File::open(input)?;
    let reader = SerializedFileReader::new(file)?;

    let parquet_metadata = reader.metadata();
    let schema_desc = parquet_metadata.file_metadata().schema_descr();
    let num_columns = schema_desc.num_columns();

    let column_descriptions: Vec<_> = (0..num_columns).map(|i| schema_desc.column(i)).collect();
    let column_names: Vec<&str> = column_descriptions
        .iter()
        .map(|col_desc| col_desc.name())
        .collect();
    let column_buf_desc: Vec<_> = column_descriptions
        .iter()
        .map(|col_desc| parquet_type_to_odbc_buffer_desc(col_desc, encoding.use_utf16()))
        .collect::<Result<_, _>>()?;
    let insert_statement = insert_statement_text(table, &column_names);

    let statement = odbc_conn.prepare(&insert_statement)?;

    let num_row_groups = reader.num_row_groups();

    // Start with a small initial batch size and reallocate as we encounter larger row groups.
    let mut batch_size = 1;
    let mut odbc_buffer = statement.into_column_inserter(
        batch_size,
        column_buf_desc.iter().map(|(desc, _copy_col)| *desc),
    )?;

    let mut pb = ParquetBuffer::new(batch_size);

    for row_group_index in 0..num_row_groups {
        info!(
            "Insert row group {} of {}.",
            row_group_index, num_row_groups
        );
        let row_group_reader = reader.get_row_group(row_group_index)?;
        let num_rows: usize = row_group_reader
            .metadata()
            .num_rows()
            .try_into()
            .expect("Number of rows in row group of parquet file must be non negative");
        // Ensure that num rows is less than batch size of originally created buffers.
        if num_rows > batch_size {
            batch_size = num_rows;
            let descs = column_buf_desc.iter().map(|(desc, _)| *desc);
            // An inefficiency here: Currently `odbc-api`s interface forces us to prepare the
            // statement again, in case we need to allocate more row groups.
            odbc_buffer = odbc_conn
                .prepare(&insert_statement)?
                .into_column_inserter(batch_size, descs)?;
        }
        odbc_buffer.set_num_rows(num_rows);
        pb.set_num_rows_fetched(num_rows);
        for (column_index, (_, parquet_to_odbc_col)) in column_buf_desc.iter().enumerate() {
            let column_reader = row_group_reader.get_column_reader(column_index)?;
            let column_writer = odbc_buffer.column_mut(column_index);
            parquet_to_odbc_col(num_rows, &mut pb, column_reader, column_writer)?;
        }

        odbc_buffer.execute()?;
    }

    Ok(())
}

fn insert_statement_text(table: &str, column_names: &[&str]) -> String {
    // Generate statement text from table name and headline
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let statement_text = format!("INSERT INTO {table} ({columns}) VALUES ({values});");
    info!("Insert statement Text: {}", statement_text);
    statement_text
}
