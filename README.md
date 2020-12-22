# Examples of apache calcite

## lineage

**input:**
`CREATE TABLE t1 AS SELECT * FROM tt1 INNER JOIN tt2 on tt1.id=tt2.fid`

**output:**

```json
{
  "sources": ["tt1", "tt2"],
  "sink": "t1"
}
```
