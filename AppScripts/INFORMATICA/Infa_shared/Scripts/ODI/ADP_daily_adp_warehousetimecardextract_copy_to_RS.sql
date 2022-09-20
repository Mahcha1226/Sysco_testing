truncate table stg.adp_warehousetimecardextract;COPY stg.adp_warehousetimecardextract



FROM 's3://sysco-dev-odi-us-east-1/ADPWhsepaycode/PBPWHSE_'
IAM_ROLE 'arn:aws:iam::782512261157:role/Application-ODI-RedshiftRole'
DELIMITER ','
ACCEPTINVCHARS AS '?'
dateformat 'auto'
timeformat 'auto'
IGNOREHEADER AS 1
REMOVEQUOTES
GZIP
;

#added new line

#secound line 
