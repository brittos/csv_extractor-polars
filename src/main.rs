use std::error::Error;
use polars::prelude::*;



fn read_csv() -> PolarsResult<DataFrame> {
    CsvReadOptions::default()
            .with_has_header(true)
            .try_into_reader_with_file_path(Some("./linkedin_job_postings.csv".into()))?
            .finish()
}

fn write_parquet(mut df: DataFrame, write_path: &str) -> Result<(), Box<dyn Error>> {
	
    let mut file = std::fs::File::create(write_path).expect("Error creating file path");
	
    ParquetWriter::new(&mut file).finish(&mut df).expect("Error writing to file");

	// If all goes `Ok`, return nothing and free the dataframe from memory
    Ok(())
}

fn main() {
	let df: DataFrame = read_csv()
		.expect("Failed to create Polars DataFrame");
	
	// Print the results
	println!("{:?}", df);

    // Parquet File
    let write_path: &str = "./linkedin_job_postings.parquet";
    let _ = write_parquet(df, &write_path)
		.expect("Unable to write to parquet");

}