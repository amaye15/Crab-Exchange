use reqwest;
use polars::prelude::*;
use std::{borrow::Borrow, io::Cursor};
use chrono::{Local, Datelike, Timelike};

// Asynchronous function to query the listing status and return a Polars DataFrame
async fn query_listing_status_to_dataframe(apikey: &str, date: Option<&str>, state: Option<&str>) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let mut request_url = format!("https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={}", apikey);

    // Optionally add the date and state to the request URL
    if let Some(date) = date {
        request_url.push_str(&format!("&date={}", date));
    }
    if let Some(state) = state {
        request_url.push_str(&format!("&state={}", state));
    }

    // Send the GET request
    let response = client.get(request_url).send().await?;

    // Check if the request was successful and get the response text
    let response_text = response.text().await?;
    let cursor = Cursor::new(response_text);

    // Read the CSV data into a Polars DataFrame
    let df = CsvReader::new(cursor)
        .infer_schema(None) // Automatically infer the schema
        .finish()?;

    Ok(df) // Return the DataFrame
}

#[tokio::main]
async fn main() {
    let apikey = "SPE2MOF2KPKKHSHH"; // Replace with your actual API key
    // let date = Some("2014-07-10");
    // let state = Some("delisted");
    let current_date = Local::now().format("%Y-%m-%d").to_string();
    let date: Option<String> = Some(current_date);
    let state: Option<&str> = Some("active");

    match query_listing_status_to_dataframe(apikey, date.as_deref(), state).await {
        Ok(df) => println!("Received DataFrame:\n{}", df),
        Err(e) => println!("Error fetching data: {}", e),
    }

    
}
