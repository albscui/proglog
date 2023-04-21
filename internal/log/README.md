# Build a Log

We build a log from the bottom up, starting with the store and index files, then segment, and finally the log. That way we can write and run tests as we build each piece.

Core compoents:

- *Record* - the data stored in our log
- *Store* - stores records
- *Index* - stores index entries
- *Segment* - the abstration that ties a store and index together
- *Log* - the abstraction that ties segments together

Each segment comprises a store file and an index file. The segment's store file is where we store the record data; we continually append records to the store. The index file is where we index each record in the store file. The index file speeds up reads because it maps record offsets to their position in the store file. Reading a record is a two step process: first yopu get the entry from the index file for the record, which tells you the position of the record in the store file, and then you read the record at the position in the store file.