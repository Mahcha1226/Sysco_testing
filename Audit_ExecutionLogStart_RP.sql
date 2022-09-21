/*****
Audit_ExecutionLogStart.sql

Input parameter: $Process (varchar(40))

1. Update target table stg.Audit_ErrorLog with finished job details where ExecutionLogID equals input parameter
2. Select and return last identity value of inserted record from step 1

*****/ 



SET time zone 'US/Central';



/***** Start the ExecutionLog for the process using the input parameter *****/

INSERT INTO stg.Audit_ErrorLog
(
	PackageName,
	StatusCode
) 
VALUES
(
	:PROCESS,
	0
)
;


/***** This returns the New Identify value created in the ExecutionLog that will be used to link to the Detail Table *****/

SELECT
	MAX(logid)
FROM
	stg.Audit_ErrorLog
WHERE
	PackageName = :PROCESS
;


