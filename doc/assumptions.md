Form and function of Assumptions yaml
=====================================

Assumptions may be represented in yaml as a dict with 3 keys: `symbols`,
`ignores` and `tables`.  Each of these is a list.  

ignores
-------
Each element is either a regular expression or a list.  If a list, each
entry in the child list is just a string, meant to be an exact match.
All column names in the source data which match either one of the regular
expressions or the simple strings should be discarded.

symbols
-------
Contains a list of `symbol` elements.  Each of these is a dict with fields
`name`, `default`, `allowed` and `usage`.   Each such dict describes how 
to replace the name (when it appears in curly braces) with one of the values in
the `allowed` list.

Something like this strategy has been used successfully in the dpdd yaml 
file (q.v.)  It's not yet clear whether it has a use in **Assumptions**.

tables
------ 
Contains a list of `table` elements. Each table is a dict with fields
`name`, `source`, `columns` and `constraints`.  Of these only `name` and 
`columns` are required.  `columns` has a list of elements, each a dict. 
There are two kinds of elements in the list: `column` and `column_group`.

For and example of all this in action, see 
[assumptions.yaml](../test/assumptions.yaml)
