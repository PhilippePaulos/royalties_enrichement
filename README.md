# Royalties Enrichment

The `royalties_enrichement` project is a Scala-based application designed to process and enrich sales data with additional album and song details.

## Overview

The main functionality of the application is encapsulated in a `Processor` class, which is an abstract class for various types of data processing tasks. The project uses a `ProcessorFactory` to create instances of specific processors based on a processor type argument provided at runtime. The processor type argument is specified as a command line argument. If no argument is provided, the default processor type is "ingestion".

The ingestion process is handled by a `SalesDataEnrichmentProcessor` class, which extends the abstract `Processor` class. The `SalesDataEnrichmentProcessor` performs the following steps:

1. Loads data from CSV files into datasets with specific encoders for album, sales, and song data.
2. Enriches the sales data with additional album and song details.
3. Writes the enriched data.

## Usage

To run the project, use the following command:

```shell
sbt run [processorType]
```
Replace [processorType] with the type of processor you want to use. If no processor type is specified, the default processor is "ingestion".

## Project Structure
- `Main.scala`: The entry point of the application. It initializes the processor based on the command line argument.
- `processor/Processor.scala`: The abstract class representing a processor.
- `processor/ProcessorFactory.scala`: The factory class that creates instances of specific processors based on the provided processor type.
- `processor/ingestion/SalesDataEnrichmentProcessor.scala`: A specific processor that loads data from CSV files and enriches sales data with additional album and song details.

## Dependencies
The project depends on Apache Spark for data processing.