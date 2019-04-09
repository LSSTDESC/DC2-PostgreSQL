Form and function of Dpdd yaml
==============================
The purpose is to take as input one or more native fields and describe how to convert to a dpdd field.  The conversion may just be a simple renaming, or it might involve operators and function calls acting on the inputs.

The yaml file consists of a list elements, each one a dict with the following fields:
* `NativeInputs`   list of one or more native field names, required
* `Datatype`       defaults to `float`
* `DPDDname`       name of output
* `Lambda`         Python lambda notation describing how to get from input(s) to output
* `RPN`            Reverse Polish for computation. Value is a list whose elements may be constants, dummy variables (e.g. x1, x2), operators or functions

The class DpddView takes a file in the above format as input and may optionally use an additional file in the same format.  Each element in the additional file is either an addition or (in the value of the `DPDDname` field matches) an override.  For ingest into PostgreSQL, this file is used to, for example, describe the `coord` field and to make use of functions available only in our PostgreSQL installation.

RPN
---
The tokens currently understood and supported by DpddView include numerical constants, dummy variables, symbolic names (enclosed in { } ) which will undergo substitution, bit operations `or`, `and`, `not`, arithemetic operators, exponentiation operator, and functions of one or two variables.
