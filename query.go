package exceltesting

const (
	getPrimaryKeyQuery = `
SELECT
	array_to_string(ARRAY_AGG(A.attname), ',')	AS	column_names
FROM
	pg_class		AS	T
,	pg_class		AS	i
,	pg_index		AS	ix
,	pg_attribute	AS	A
,	pg_tables		AS	ta
WHERE
	T.oid			=	ix.indrelid
AND	i.oid			=	ix.indexrelid
AND	ix.indisprimary	=	TRUE
AND	A.attrelid		=	T.oid
AND	A.attnum		=	ANY(ix.indkey)
AND	T.relkind		IN	('r', 'p')
AND	T.relname		=	ta.tablename
AND	ta.schemaname	=	CURRENT_SCHEMA()
AND	T.relname		=	$1
GROUP BY
	T.relname
,	i.relname
,	ix.indisprimary
ORDER BY
	T.relname
,	i.relname
;`

	getTableNotNullColumns = `
SELECT
	column_name
,	udt_name
FROM
	information_schema.columns
WHERE
	table_schema	=	CURRENT_SCHEMA()
AND	table_name		=	$1
AND	is_nullable		=	'NO'
/*
	If column_default exists, no explicit value needs to be specified.
	For example, the automatic incremental types such as serial, bigserial, etc. are applicable.
*/
AND	column_default	IS	NULL
ORDER BY
	ordinal_position
;
`
)
