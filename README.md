# Diomede

An LMDB-based RDF quadstore.

## Table of Contents

* [Table of Contents](#table-of-contents)
* [Description](#description)
* [Data Layout](#data-layout)
* [Term Encoding](#term-encoding)
* [API and Design Choices](#api-and-design-choices)

## Description

Diomede is an RDF quadstore written in Swift.
It uses [LMDB](http://www.lmdb.tech/) as its underlying storage engine, and is designed to integrate into the [Kineo](https://github.com/kasei/kineo) SPARQL engine.
The use of LMDB, and the specific data layout used is meant to be both extensible and usable in a progressive manner in which simple tasks can be accomplished with only a subset of the database structure.

## Data Layout

The LMDB file structure is organized into a number of named databases.
The required databases are:

* `quads`

	This is the primary table representing quads, mapping a quad ID (8-byte integer) to four term IDs (4 concatenated 8-byte integers).
	Term IDs are stored in subject-predicate-object-graph order.

* `fullIndexes`

	This is a database containing a list of all the (optional) quad-ordering indexes, mapping the index name (permutations of "spog") to an array containing the ordinal of each term position in the index order.
	The ordinals are represented as 8-byte integers and must be a permutation of `[0,1,2,3]` (which itself represents the subject-predicate-object-graph order).
	An entry in this database implies the existence of a database whose name is the entry's key.

* `term_to_id`

	This is a mapping from the SHA256 hash of [encoded term values](#term-encoding) to term IDs (8-byte integers).

* `id_to_term`

	This is a mapping from term IDs (8-byte integers) to [encoded term values](#term-encoding).

* `graphs`

	This is a table of the named graphs present in the database, mapping term IDs to empty (0-byte) values.
	Its data is redundant, being computable from the unique terms represented by the graph position of each record in `quads` table.

* `stats`

	This is a table of metadata useful to either/both the Diomede system or to end-users.
	Some keys that are present are:
	
	* `Version`
	* `Last-Modified`
	* `next_unassigned_term_id`
	* `next_unassigned_quad_id`

Optional databases used for indexing may also be present:

* Any "full index" databases named with a permutation of "spog" (e.g. `spog` and `pogs`)

	These databases map four term IDs (4 concatenated 8-byte integers) to a quad ID (8-byte integer) in the order implied by the database name (and given explicitly as the value of the corresponding entry in the `fullIndexes` database).

* `characteristicSets`

	This is a database containing an encoding of the [Characteristic Sets](http://www.csd.uoc.gr/~hy561/papers/storageaccess/optimization/Characteristic%20Sets.pdf) for each named graph in the database.
	The keys in the database are a pair (graph term ID, sequence number), encoded as 2 concatenated 8-byte integers.
	The values are arrays of 8-byte integers in which the first element is the total cardinality for the Characteristic Set, and the remaining elements are (predicate term ID, occurrence count) pairs.

## Term Encoding

The encoding of RDF terms (performed in [RDFExtensions.swift](Sources/DiomedeQuadStore/RDFExtensions.swift)) produces a UTF-8 encoded string which is either stored in the database (in `id_to_term`) or hashed with SHA256 and stored (in `term_to_id`).
The encodings depend on the term type, but all identify the type with the first character.

* IRIs

	* LATIN CAPITAL LETTER I (U+0049)
	* QUOTATION MARK (U+0022)
	* IRI value

* Blank Nodes
	* LATIN CAPITAL LETTER B (U+0042)
	* QUOTATION MARK (U+0022)
	* Blank node identifier

* Language Literals

	* LATIN CAPITAL LETTER L (U+004C)
	* Language tag
	* QUOTATION MARK (U+0022)
	* Literal value

* `xsd:integer` Literals

	* LATIN SMALL LETTER I (U+0069)
	* QUOTATION MARK (U+0022)
	* Integer string value

* Other Datatype Literals

	* LATIN CAPITAL LETTER D (U+0044)
	* Datatype IRI value
	* QUOTATION MARK (U+0022)
	* Literal value

Note that no canonicalization or unicode normalization is performed.

## API and Design Choices

All integers are 8-bytes and stored as big-endian.

Effort is made to keep LMDB transactions short-lived.
This means that matching operations are generally performed atomically, materializing an entire list of term or quad IDs.
However, in an attempt to benefit from some degree of pipelining and avoid unnecessary work (e.g. if a limited number of matches is requested), materializing term values is performed in batches, with each batch being processed in its own read transaction.
This is assumes that terms are never deleted from the `term_to_id` and `id_to_term` databases.

The term ID lookup databases (`term_to_id` and `id_to_term`) use both hashing and an assigned integer for each term.
While this adds complexity, it is done for several reasons:

* The use of hashing allows keys in the `term_to_id` database to be fixed size, and remain below the LMDB key size limit (which is a compile-time constant which defaults to 511 bytes).
* The use of integers as the primary key for a term (instead of the hash values) allows some flexibility in how IDs are assigned and used.
	
	Future development of this format may inline frequently-occurring terms, or those with minimal size requirements.
	This can reduce the work performed in materializing terms during query processing.
	
	It is also expected that the ability to assign term IDs will be useful in supporting RDF* in the future by inlining quad ID values in term IDs.
